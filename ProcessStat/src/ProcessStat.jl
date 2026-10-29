"""
    ProcessStat

Convert the JSONL written by `pipelines/stat.py` (one `os.stat` result per
line) into one file per `code`, in an output directory:

  * `code_0.arrow`    every line with `code == 0`, as an Arrow IPC file with
                      the columns `name`, `path`, `st_size`, `st_atime`,
                      `st_mtime`, `st_ctime` (`code` is constant, so omitted)
  * `code_N.jsonl`    the raw lines of every other integer code `N`
  * `unparsed.jsonl`  lines that are not JSON, have no integer `code`, or
                      carry a code-0 record that is not decoded (see
                      `sinkname` and `badsurrogate`)

`convert_file` is the entry point; the knobs are documented at `settings`.

# Pipeline

W worker tasks each `read chunk -> parse -> Arrow-encode + zstd`, all in
parallel; the calling task only puts the finished record-batch `Message`s,
in chunk order, into the `Arrow.Writer`, whose own task writes the bytes and,
on close, the footer.
"""
module ProcessStat

using Arrow, JSON, Tables
using Printf
using PrecompileTools: @setup_workload, @compile_workload

# ---- knobs ---------------------------------------------------------------------------------------

"Arrow buffer compression: `:zstd`, `:lz4` or `nothing`."
const COMPRESS = :zstd

"""
    envint(name, default) -> Int

The integer in the environment variable `name`, or `default` when it is unset
or blank.
"""
function envint(name, default::Int)
    s = strip(get(ENV, name, ""))
    return isempty(s) ? default : parse(Int, s)
end

"""
    settings(; chunk_mb, inflight, window, encode) -> NamedTuple

Resolve the conversion knobs. Every keyword falls back to the environment
variable named next to it, and that to the default:

  * `chunk_mb`  (`PROCESSSTAT_CHUNK_MB`, default 8)
    JSONL MB per chunk = per Arrow record batch
  * `inflight`  (`PROCESSSTAT_INFLIGHT`, default 8)
    worker tasks = chunks parsed/encoded at once;
    W = min(inflight, Threads.nthreads())
  * `window`    (`PROCESSSTAT_WINDOW`, default 2W)
    chunks claimed but not yet handed to the writer
  * `encode`    (`PROCESSSTAT_ENCODE`, default `parallel`)
    `serial` = `Arrow.write` per batch on the calling task (slower; the test
    oracle for the parallel path)

Returns `(; chunk_bytes, nworkers, window, serial)`, the form `_convert`
takes. Throws an `ArgumentError` for a value out of range.

# Memory model

Peak memory footprint ≈ 0.45 GB + 7 × W × CHUNK (measured, macOS, zstd):

  * 0.45 GB: Julia runtime + loaded code (the `@compile_workload` at the end
    of the module avoids ~0.35 GB of JIT)
  * per worker: read buffer (1×) + the chunk's columns (~0.7×) + zstd output
    (≤1×) + GC slack
  * waiting: up to window − W finished batches + 2 in Arrow's queue, each only
    its compressed body (~CHUNK/8) thanks to `unpin`

Defaults (8 MB, W = 8): ~0.95 GB at any `-t ≥ 8` on the 60 GB file. Nothing
depends on the file size or on `Threads.nthreads()` beyond `inflight`. Safe
upper bound for sizing: 0.5 GB + 8 × W × CHUNK.
"""
function settings(;
        chunk_mb::Integer = envint("PROCESSSTAT_CHUNK_MB", 8),
        inflight::Integer = envint("PROCESSSTAT_INFLIGHT", 8),
        window::Union{Nothing,Integer} = nothing,
        encode::Symbol = Symbol(get(ENV, "PROCESSSTAT_ENCODE", "parallel"))
    )

    encode in (:parallel, :serial) || throw(
        ArgumentError("encode=$encode must be :parallel or :serial")
    )

    1 <= chunk_mb <= 1024 || throw(
        ArgumentError("chunk_mb=$chunk_mb must be in 1:1024")
    )

    inflight >= 1 || throw(
        ArgumentError("inflight=$inflight must be >= 1")
    )

    nworkers = min(Threads.nthreads(), inflight)
    window   = something(window, envint("PROCESSSTAT_WINDOW", 2 * nworkers))
    window >= nworkers || throw(
        ArgumentError("window=$window must be >= workers ($nworkers)")
    )

    return (;
        chunk_bytes = Int(chunk_mb) * 2^20,
        nworkers,
        window      = Int(window),
        serial      = encode === :serial
    )
end

# ---- code == 0 rows ------------------------------------------------------------------------------

"""
    Entry0

One `code == 0` line of `stat.py` as JSON.jl decodes it (the slow path, see
`handle_line_json!`). The field names are the JSON keys.
"""
struct Entry0
    name::String
    path::String
    code::Int64
    st_size::Int64
    st_atime::Float64
    st_mtime::Float64
    st_ctime::Float64
end

"""
    StrCol

A string column in Arrow's own layout: all values back to back in `bytes`,
plus `Int32` end offsets with `offs[1] = 0`, so that value `k` is
`bytes[offs[k]+1 : offs[k+1]]`.
"""
struct StrCol
    bytes::Vector{UInt8}
    offs::Vector{Int32}
end

"""
    StrCol(nbytes, n)

An empty column with room reserved for `n` values totalling `nbytes`.
"""
StrCol(nbytes::Int, n::Int) = StrCol(
    sizehint!(UInt8[], nbytes),
    sizehint!(Int32[0], n + 1)
)

"""
    pushbytes!(c::StrCol, src, lo, n) -> c

Append the `n` bytes of `src` starting at index `lo` as one more value. `src`
is anything `pointer(src, i)` works on (a `Vector{UInt8}` or a `String`).
Throws when the column would outgrow its `Int32` offsets.
"""
@inline function pushbytes!(c::StrCol, src, lo::Int, n::Int)
    m = length(c.bytes)
    m + n <= typemax(Int32) || error(
        "string data of one chunk exceeds 2 GiB (Int32 offsets)"
    )

    resize!(c.bytes, m + n)
    n > 0 && GC.@preserve c src unsafe_copyto!(
        pointer(c.bytes, m + 1), pointer(src, lo), n
    )

    push!(c.offs, Int32(m + n))
    return c
end

# whole strings, for the JSON.jl path (`pushentry!`)
Base.push!(c::StrCol, s::String) = pushbytes!(c, s, 1, ncodeunits(s))

"""
    newcolumns(nbytes, n) -> Columns

The empty column set of one record batch, sized for `n` rows parsed from
`nbytes` of JSONL (names get `nbytes ÷ 8`, paths `nbytes ÷ 2`).
"""
function newcolumns(nbytes::Int, n::Int)
    cols = (
        name     = StrCol(nbytes ÷ 8, n),
        path     = StrCol(nbytes ÷ 2, n),
        st_size  = Int64[],
        st_atime = Float64[],
        st_mtime = Float64[],
        st_ctime = Float64[]
    )

    foreach(
        c -> sizehint!(c, n),
        (cols.st_size, cols.st_atime, cols.st_mtime, cols.st_ctime)
    )

    return cols
end

"""
    Columns

The column set of one record batch: a `NamedTuple` with `name` and `path`
(`StrCol`) and `st_size`, `st_atime`, `st_mtime`, `st_ctime` (vectors). The
constant `code` is left out; it is in the file name.
"""
const Columns = typeof(newcolumns(0, 0))

"""
    nrows(c::Columns) -> Int

Number of rows in `c`.
"""
nrows(c::Columns) = length(c.st_size)

"""
    pushentry!(c::Columns, e::Entry0) -> c

Append the row `e` (the JSON.jl path; the fast path pushes the fields straight
from the bytes, see `handle_line!`).
"""
@inline function pushentry!(c::Columns, e::Entry0)
    push!(c.name, e.name)
    push!(c.path, e.path)
    push!(c.st_size, e.st_size)
    push!(c.st_atime, e.st_atime)
    push!(c.st_mtime, e.st_mtime)
    push!(c.st_ctime, e.st_ctime)
    return c
end

# ---- everything else: raw lines, one file per code -----------------------------------------------

"""
    Sinks(dir)

The side files of a run: one `IOStream` per `code` (`nothing` for
`unparsed.jsonl`), each opened in `dir` on first use, a line count per code,
and the lock that serialises `stash!`.
"""
struct Sinks
    dir::String
    lock::ReentrantLock
    files::Dict{Union{Int,Nothing},IOStream}
    counts::Dict{Union{Int,Nothing},Int}
end
Sinks(dir) = Sinks(
    dir,
    ReentrantLock(),
    Dict{Union{Int,Nothing},IOStream}(),
    Dict{Union{Int,Nothing},Int}()
)

"""
    sinkname(code) -> String

File name of the side file for `code`: `code_N.jsonl`, or `unparsed.jsonl`
for `nothing`.

`unparsed.jsonl` holds the lines that are not JSON or have no integer `code`,
and also the code-0 lines whose `name`/`path` holds bytes that are not UTF-8
(see `badsurrogate`), so totals computed from `code_0.arrow` alone miss those
entries.
"""
sinkname(code) = code === nothing ? "unparsed.jsonl" : "code_$(code).jsonl"

"""
    stash!(s::Sinks, data, lo, hi, code)

Write `data[lo:hi]` plus a newline to the side file of `code`, opening it if
needed, and count it. Rare path, so one lock is fine. Lines from different
chunks interleave; sort later if you care.
"""
function stash!(s::Sinks, data::Vector{UInt8}, lo::Int, hi::Int, code)
    lock(s.lock) do
        io = get!(
            () -> open(joinpath(s.dir, sinkname(code)), "w"),
            s.files, code
        )
        GC.@preserve data unsafe_write(io, pointer(data, lo), hi - lo + 1)
        write(io, '\n')
        s.counts[code] = get(s.counts, code, 0) + 1
    end
end

"""
    isoutput(f) -> Bool

Whether the file name `f` is one a previous run into the same outdir may have
left behind. Side files are only created when a line with that code shows up,
so without removing these a re-run could keep stale ones.
"""
isoutput(f) = (
    f in ("code_0.arrow", "code_0.arrow.partial", "unparsed.jsonl") ||
    occursin(r"^code_-?\d+\.jsonl$", f)
)

"""
    CheckedIO(io::IOStream)

An `IOStream` whose short writes are errors. Arrow.jl ignores the byte counts
`IOStream` returns (`IOStream` reports a short write instead of throwing, e.g.
on a full disk), which could publish a corrupt file.
"""
struct CheckedIO <: IO
    io::IOStream
end
function Base.unsafe_write(c::CheckedIO, p::Ptr{UInt8}, n::UInt)
    m = unsafe_write(c.io, p, n)
    m == n || throw(SystemError("writing $(c.io.name)", Libc.errno()))
    return m
end
function Base.write(c::CheckedIO, b::UInt8)
    write(c.io, b) == 1 || throw(
        SystemError("writing $(c.io.name)", Libc.errno())
    )
    return 1
end
Base.position(c::CheckedIO) = position(c.io)
Base.isopen(c::CheckedIO)   = isopen(c.io)
Base.flush(c::CheckedIO)    = flush(c.io)
Base.close(c::CheckedIO)    = close(c.io)

# ---- line handling -------------------------------------------------------------------------------

include("fastparse.jl")

"""
    badsurrogate(l::AbstractVector{UInt8}) -> Bool

Whether the JSON line `l` holds a `\\uD800`-`\\uDFFF` escape that is not a
high+low surrogate pair.

JSON.jl 1.9 mis-decodes such escapes (it re-emits a hex digit and pairs
low+low / high+high), and `stat.py` writes one `\\udcXX` per filename byte
that is not UTF-8 (`os.fsdecode` surrogateescape). Such lines go to
`unparsed.jsonl` verbatim instead of becoming a corrupted name; `json.loads` +
`os.fsencode` in Python recovers the original bytes.
"""
function badsurrogate(l::AbstractVector{UInt8})
    n = length(l)
    function kind(i) # 0: no \uD8xx-\uDFxx escape at i, 1: high, 2: low
        (
            i + 5           <= n           &&
            l[i]            == UInt8('\\') &&
            l[i+1]          == UInt8('u')  &&
            (l[i+2] | 0x20) == UInt8('d')
        ) || return 0

        c = l[i+3] | 0x20
        return c in codeunits("89ab") ? 1 : c in codeunits("cdef") ? 2 : 0
    end

    i = 1
    while i <= n
        if l[i] != UInt8('\\')
            i += 1
        else
            k = kind(i)
            k == 0 && (i += 2; continue) # other escape, incl. an escaped backslash
            (k == 1 && kind(i + 6) == 2) || return true
            i += 12
        end
    end
    return false
end

"""
    handle_line_json!(cols::Columns, sinks::Sinks, data, lo, hi)

Handle the line `data[lo:hi]` with the original, JSON.jl based logic; used for
every line the fast path (`fastparse`) does not take. A `code == 0` line is
appended to `cols`; anything else is stashed in the side file of its code, or
in `unparsed.jsonl` when there is no integer code, the record does not decode,
or `badsurrogate` says JSON.jl would store a corrupt name.

The catch-alls rethrow ^C: without an interactive thread it can be delivered
to a worker here.
"""
function handle_line_json!(
        cols::Columns, sinks::Sinks, data::Vector{UInt8}, lo::Int, hi::Int
    )
    line = view(data, lo:hi)
    code = try
        JSON.lazy(line).code[] # navigate to "code", materialize only that value
    catch e
        e isa InterruptException && rethrow()
        nothing                # no such key / malformed → unparsed.jsonl
    end

    if code == 0
        # JSON.jl would store a corrupt name
        badsurrogate(line) && return stash!(sinks, data, lo, hi, nothing)
        try
            pushentry!(cols, JSON.parse(line, Entry0))
        catch e
            e isa InterruptException && rethrow()
            stash!(sinks, data, lo, hi, nothing)
        end
    else
        # an integer code outside Int64 (never written by stat.py) goes to
        # unparsed.jsonl instead of aborting the whole conversion with an
        # InexactError
        fits = code isa Integer && typemin(Int) <= code <= typemax(Int)
        stash!(sinks, data, lo, hi, fits ? Int(code) : nothing)
    end
end

"""
    handle_line!(cols::Columns, sinks::Sinks, data, lo, hi)

Handle the line `data[lo:hi]` (no newline, trailing blanks trimmed): the fast
path appends it to `cols` straight from the bytes, otherwise it is handed to
`handle_line_json!`.
"""
@inline function handle_line!(
        cols::Columns, sinks::Sinks, data::Vector{UInt8}, lo::Int, hi::Int
    )
    ok, nlo, nhi, plo, phi, sz, at, mt, ct = fastparse(data, lo, hi)
    ok || return handle_line_json!(cols, sinks, data, lo, hi)

    pushbytes!(cols.name, data, nlo, nhi - nlo + 1)
    pushbytes!(cols.path, data, plo, phi - plo + 1)
    push!(cols.st_size, sz)
    push!(cols.st_atime, at)
    push!(cols.st_mtime, mt)
    push!(cols.st_ctime, ct)

    return
end

"""
    process_chunk(data, r::UnitRange{Int}, sinks::Sinks) -> Columns

Parse the chunk `data[r]` (which ends on a newline or at EOF) line by line
into a fresh `Columns`; lines with other codes go to `sinks`. Blank lines are
skipped and trailing `' '`/`'\\r'` are trimmed. Runs on a worker thread.
"""
function process_chunk(data::Vector{UInt8}, r::UnitRange{Int}, sinks::Sinks)
    lo, hi = first(r), last(r)
    # lines are ~190-300 bytes; over-estimating is harmless
    cols = newcolumns(length(r), length(r) ÷ 150)
    pos  = lo
    while pos <= hi
        nl   = findnext(==(UInt8('\n')), data, pos)      # memchr
        stop = (nl === nothing || nl > hi) ? hi : nl - 1 # last byte before '\n'
        e    = stop
        while e >= pos && (data[e] == UInt8('\r') || data[e] == UInt8(' '))
            e -= 1
        end
        e >= pos && handle_line!(cols, sinks, data, pos, e) # skips blank lines
        pos = stop + 2
    end
    return cols
end

"""
    chunk_ranges(path, chunk_bytes) -> Vector{UnitRange{Int}}

Split the file at `path` into ranges of about `chunk_bytes` bytes that each
end on a newline (or at EOF), using small probe reads instead of an mmap.
"""
function chunk_ranges(path::AbstractString, chunk_bytes::Int)
    n, ranges = filesize(path), UnitRange{Int}[]
    probe = Vector{UInt8}(undef, 64 * 2^10)
    open(path) do io
        lo = 1
        while lo <= n
            hi = lo + chunk_bytes - 1
            if hi >= n
                hi = n
            else
                seek(io, hi - 1)               # byte hi (1-based) lives at offset hi-1
                while true                     # move hi forward to the next '\n'
                    m = readbytes!(io, probe)
                    m == 0 && (hi = n; break)
                    k = findfirst(==(UInt8('\n')), view(probe, 1:m))
                    k === nothing || (hi += k - 1; break)
                    hi += m
                end
            end
            push!(ranges, lo:hi)
            lo = hi + 1
        end
    end
    return ranges
end

# ---- Arrow.jl 2.8 internals (all of them are in this section) ------------------------------------

# The fields of Arrow.Writer this section reads (checked one by one), and the
# exact layouts of Arrow.Compressed and Arrow.Message it rebuilds in `unpin`
# and `encode`.
const FIELD_NAME_ALLOWED    = (
    :msgs, :task, :schema, :firstcols, :isclosed, :closeio, :io, :compress,
    :largelists, :denseunions, :maxdepth, :colmeta, :alignment
)
const FIELD_NAME_COMPRESSED = (:data, :buffers, :len, :nullcount, :children)
const FIELD_NAME_MESSAGE    = (
    :msgflatbuf, :columns, :bodylen, :isrecordbatch, :blockmsg, :headerType
)

"""
    check_arrow()

Verify that the installed Arrow.jl still has the internals this section relies
on; throws with a "ProcessStat needs updating" message otherwise.

Field names catch structural changes; the byte comparison of a file written
via `Arrow.write` with one written via `encode` + `unpin` catches behavioural
ones (e.g. reordered positional arguments of `toarrowtable`), which would
otherwise produce a corrupt `code_0.arrow`. It also runs in the
`@compile_workload`, so an incompatible Arrow.jl already fails at precompile
time.
"""
function check_arrow()
    for f in FIELD_NAME_ALLOWED
        hasfield(Arrow.Writer, f) || error(
            "Arrow.jl $(pkgversion(Arrow)): Writer.$f missing; ProcessStat needs updating"
        )
    end

    layout_ok = fieldnames(Arrow.Compressed) == FIELD_NAME_COMPRESSED &&
                fieldnames(Arrow.Message)    == FIELD_NAME_MESSAGE
    layout_ok || error(
        "Arrow.jl $(pkgversion(Arrow)): unexpected internal layout; ProcessStat needs updating"
    )

    c1 = newcolumns(0, 0)
    c2 = newcolumns(0, 0)
    pushentry!(c1, Entry0("a", "/a", 0, 1, 1.5, 2.0, 3.0))
    pushentry!(c2, Entry0("", "/b", 0, 2, 0.0, -1.0, 1.0e9))

    function viawriter(internals::Bool)
        io = IOBuffer()
        w  = open(Arrow.Writer, io; compress=COMPRESS, ntasks=2)
        Arrow.write(w, arrowcols(c1))
        if internals
            w.firstcols[] = unpin(w.firstcols[])
            try
                put!(w.msgs, encode(c2, w)[1])
            finally
                close(w.msgs)
            end
            wait(w.task)
        else
            Arrow.write(w, arrowcols(c2))
        end
        close(w)
        return take!(io)
    end

    viawriter(true) == viawriter(false) || error(
        "Arrow.jl $(pkgversion(Arrow)): internals no longer reproduce " *
        "Arrow.write; ProcessStat needs updating (pin Arrow to 2.8.1 meanwhile)"
    )
    return
end

"""
    arrowlist(c::StrCol) -> Arrow.List

Wrap `c` as an Arrow string column without copying. Arrow passes a ready
`List` through unchanged, which skips its `ToList` flatten (a binary search
per byte: ~90% of what used to look like zstd time).
"""
arrowlist(c::StrCol) = Arrow.List{String,Int32,Vector{UInt8}}(
    UInt8[],
    Arrow.ValidityBitmap(UInt8[], 1, length(c.offs) - 1, 0),
    Arrow.Offsets{Int32}(UInt8[], c.offs),
    c.bytes,
    length(c.offs) - 1,
    nothing
)

"""
    arrowcols(c::Columns) -> NamedTuple

The columns of `c` as the table `Arrow.write` and `encode` take: the string
columns via `arrowlist`, the numeric vectors as they are.
"""
arrowcols(c::Columns) = (
    name     = arrowlist(c.name),
    path     = arrowlist(c.path),
    st_size  = c.st_size,
    st_atime = c.st_atime,
    st_mtime = c.st_mtime,
    st_ctime = c.st_ctime
)

"""
    emptylike(x)

An empty column of the same type (and metadata) as the Arrow column `x`. For
column types `unpin` does not know, `x` itself (correct, only pins memory).
"""
emptylike(x::Arrow.Primitive{T,D}) where {T,D<:Vector} = Arrow.Primitive{T,D}(
    UInt8[],
    Arrow.ValidityBitmap(UInt8[], 1, 0, 0),
    D(),
    0,
    x.metadata
)
emptylike(::Type{D}) where {D<:Vector} = D()
emptylike(x::Arrow.List{T,O,D}) where {T,O,D<:Vector} = Arrow.List{T,O,D}(
    UInt8[],
    Arrow.ValidityBitmap(UInt8[], 1, 0, 0),
    Arrow.Offsets{O}(UInt8[], O[0]),
    emptylike(D),
    0,
    x.metadata
)
emptylike(x) = x

"""
    shrink(b::Arrow.CompressedBuffer)

A copy of `b` whose data has no spare capacity (`transcode` leaves each
compressed buffer with the capacity of its input).
"""
shrink(b::Arrow.CompressedBuffer) = Arrow.CompressedBuffer(
    copy(b.data),
    b.uncompressedlength
)

"""
    unpin(c)

Drop what a finished record batch still pins: a compressed column keeps a
reference to its source column, and each compressed buffer the capacity of its
input. After `unpin` a queued batch costs only its body. Recurses through
`Arrow.Compressed` columns and `Arrow.ToArrowTable`; anything else is returned
as it is.
"""
unpin(c::Arrow.Compressed{Z,D}) where {Z,D} = Arrow.Compressed{Z,D}(
    emptylike(c.data),
    map(shrink, c.buffers),
    c.len,
    c.nullcount,
    Arrow.Compressed[unpin(ch) for ch in c.children]
)
unpin(c) = c
unpin(t::Arrow.ToArrowTable) = Arrow.ToArrowTable(
    t.sch,
    Any[unpin(c) for c in t.cols],
    t.metadata,
    t.dictencodingdeltas
)

"""
    encode(cols::Columns, w::Arrow.Writer) -> (msg, schema, nrows)

Encode and compress one chunk into the record-batch `Arrow.Message` that
`Arrow.write(w, ...)` would produce, but on the calling (worker) task and with
the batch unpinned. Dictionary encoding is off, so concurrent calls are safe
(the compressors are per-thread `Lockable`s). `schema` is returned for the
caller to compare with `w.schema[]`.
"""
function encode(cols::Columns, w::Arrow.Writer)
    tbl = Arrow.toarrowtable(
        arrowcols(cols),
        Dict{Int64,Any}(), # dictencodings
        w.largelists,
        w.compress,
        w.denseunions,
        false,             # dictencode
        false,             # dictencodenested
        w.maxdepth,
        nothing,           # meta
        w.colmeta
    )
    msg = Arrow.makerecordbatchmsg(tbl.sch, tbl, w.alignment)
    msg = Arrow.Message(
        msg.msgflatbuf,
        unpin(tbl),        # columns
        msg.bodylen,
        msg.isrecordbatch,
        msg.blockmsg,
        msg.headerType
    )
    return (msg, tbl.sch, nrows(cols))
end

"""
    abort!(w::Arrow.Writer)

Stop the writer after an error without ever throwing; the output stays
`code_0.arrow.partial`.
"""
function abort!(w::Arrow.Writer)
    try
        if !w.isclosed
            close(w.msgs)
            try wait(w.task) catch end
            try w.closeio && close(w.io) catch end
            w.isclosed = true
        end
    catch
    end
    return
end

# ---- driver --------------------------------------------------------------------------------------

"""
    convert_file(infile, outdir; chunk_mb, inflight, window, encode)

Convert the `stat.py` JSONL `infile` into `outdir/code_0.arrow` plus one
`code_N.jsonl` per other code and `unparsed.jsonl` (the module docstring says
what goes where). Outputs of an earlier run in `outdir` are removed first. The
keywords are the knobs of `settings`.

`infile` must be a regular file (chunks are read with `seek`). The Arrow file
is written as `code_0.arrow.partial` and renamed once the footer is on disk,
so a failed or killed run never leaves a footer-less file that looks finished.
"""
convert_file(infile::AbstractString, outdir::AbstractString; kw...) = _convert(
    infile, outdir, settings(; kw...)
)

"""
    _convert(infile, outdir, cfg)

The conversion behind `convert_file`, taking the resolved `cfg` of `settings`
(the `@compile_workload` calls it with a hand-made one).

Deadlock-freedom: chunks are claimed in order and only claimed-but-unwritten
chunks hold tokens, so if chunk `next` is unclaimed a token is free; if all
workers died, `results` is closed. `results` never blocks a worker (at most
`window` chunks are alive), and `pending` ≤ window.
"""
function _convert(infile::AbstractString, outdir::AbstractString, cfg)
    check_arrow()
    isfile(infile) || throw(ArgumentError(
        "$infile is not a regular file (pipes / process substitution are " *
        "not supported: chunks are read with seek)"
    ))
    fsize  = filesize(infile)
    # reads the input: fails before outdir is touched
    ranges = chunk_ranges(infile, cfg.chunk_bytes)

    mkpath(outdir)
    stale = filter(isoutput, readdir(outdir))
    # the run would truncate it (outputs are opened "w"), so refuse instead
    # of keeping it
    for f in stale
        samefile(joinpath(outdir, f), infile) && throw(ArgumentError(
            "$infile is (or is linked as) the output file $f in $outdir; " *
            "use another outdir"
        ))
    end
    isempty(stale) || @info "removing outputs of an earlier run" outdir stale
    foreach(f -> rm(joinpath(outdir, f)), stale)

    nchunks  = length(ranges)
    maxlen   = maximum(length, ranges; init=0)
    nworkers = cfg.nworkers
    sinks    = Sinks(outdir)
    # a Channel, not a Semaphore: close() wakes waiters
    tokens   = Channel{Nothing}(cfg.window)
    foreach(_ -> put!(tokens, nothing), 1:cfg.window)
    results  = Channel{Tuple{Int,Any}}(cfg.window)
    nexti    = Threads.Atomic{Int}(1) # next chunk to claim
    @info(
        "converting",
        infile,
        GB = round(fsize / 1e9; digits=2),
        nchunks,
        chunk_MB = cfg.chunk_bytes / 2^20,
        nworkers,
        cfg.window,
        Threads.nthreads()
    )

    # Written under a temporary name and renamed once the footer is on disk, so
    # a killed run (SIGTERM, OOM, ^C) never leaves a footer-less file that
    # looks like a finished code_0.arrow.
    arrowfile = joinpath(outdir, "code_0.arrow")
    tmpfile   = arrowfile * ".partial"
    writer    = open(
        Arrow.Writer, CheckedIO(open(tmpfile, "w"));
        compress=COMPRESS, ntasks=2, closeio=true
    )
    bind(writer.msgs, writer.task)

    workers = map(1:nworkers) do _
        Threads.@spawn try
            open(infile) do io                     # own handle per worker
                buf = Vector{UInt8}(undef, maxlen) # reused for every chunk
                while true
                    take!(tokens) # blocks while `window` chunks are alive
                    i = Threads.atomic_add!(nexti, 1)
                    i > nchunks && (put!(tokens, nothing); break)
                    r = ranges[i]
                    seek(io, first(r) - 1)
                    readbytes!(io, buf, length(r)) == length(r) || error(
                        "short read in chunk $i"
                    )
                    cols    = process_chunk(buf, 1:length(r), sinks)
                    payload = (i == 1 || cfg.serial) ? cols : encode(cols, writer)
                    put!(results, (i, payload))
                    yield()
                end
            end
        catch e
            isopen(results) && close(
                results, CapturedException(e, catch_backtrace())
            )
            rethrow()
        end
    end

    # early arrivals, never more than `window`
    pending = Dict{Int,Any}()
    rows0   = 0
    t0      = time()
    tlast   = 0.0
    try
        # a valid 0-row file
        nchunks == 0 && Arrow.write(writer, arrowcols(newcolumns(0, 0)))
        for next in 1:nchunks
            while !haskey(pending, next)
                i, p = take!(results)
                pending[i] = p
            end
            p = pop!(pending, next)
            if next == 1 || cfg.serial # public API path
                Arrow.write(writer, arrowcols(p::Columns))
                # the footer needs only the types
                next == 1 && (writer.firstcols[] = unpin(writer.firstcols[]))
                rows0 += nrows(p)
            else
                msg, sch, n = p
                sch == writer.schema[] || error(
                    "chunk $next: schema $sch != $(writer.schema[])"
                )
                # blocks while 2 are queued; throws if the IO task died
                put!(writer.msgs, msg)
                rows0 += n
            end
            p = nothing
            put!(tokens, nothing)
            if time() - tlast > 0.5 || next == nchunks
                tlast = time()
                done  = last(ranges[next])
                @printf(
                    stderr, "\r%5.1f%%  %6.2f GB  %5.0f MB/s ",
                    100done / fsize, done / 1e9, done / 1e6 / (tlast - t0)
                )
            end
        end
        # wait throws if Arrow's IO task died (e.g. a short write: disk full)
        close(writer.msgs)
        wait(writer.task)
        close(writer) # footer
        foreach(wait, workers)
        # flushes the side files; throws on a write error
        foreach(close, values(sinks.files))
        # only now does the run count as finished
        mv(tmpfile, arrowfile; force=true)
    catch
        close(tokens)
        close(results)
        abort!(writer)
        foreach(t -> try wait(t) catch end, workers)
        foreach(f -> try close(f) catch end, values(sinks.files))
        rethrow()
    end
    println(stderr)
    @info(
        "done",
        rows_code0  = rows0,
        other_codes = sinks.counts,
        seconds     = round(time() - t0; digits=1)
    )
end

@setup_workload begin
    lines = [
        """{"name":"a","path":"/a","code":0,"st_size":1,"st_atime":1.5,"st_mtime":2.0,"st_ctime":3.0}""",
        """{"name":"b\\u00e9","path":"/a/b","code":0,"st_size":2,"st_atime":0.0,"st_mtime":2.0,"st_ctime":3.0}""",
        """{"name":"c","path":"/a/c","code":13,"error":"Permission denied"}""",
        """{"name":"d","path":"/a/d","code":0,"st_size":"oops"}""",
        """not json""",
    ]
    @compile_workload begin
        mktempdir() do dir
            infile = joinpath(dir, "in.jsonl")
            write(infile, join(repeat(lines, 40), '\n'), '\n')
            cfg = (; chunk_bytes = 1024, nworkers = 2, window = 4, serial = false)
            Base.CoreLogging.with_logger(Base.CoreLogging.NullLogger()) do
                redirect_stderr(devnull) do
                    _convert(infile, joinpath(dir, "out"), cfg)
                end
            end
        end
    end
end

end # module ProcessStat
