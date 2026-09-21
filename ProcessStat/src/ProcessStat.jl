module ProcessStat

using Arrow, JSON, Tables
using Mmap, Printf

const CHUNK_BYTES = 64 * 2^20   # JSONL bytes per task = per record batch. Peak RSS ≈ 2–3 × nthreads × this.
const COMPRESS    = :zstd       # :zstd | :lz4 | nothing

# ---- code == 0 ---------------------------------------------------------------------------------

struct Entry0
    name::String
    path::String
    code::Int64
    st_size::Int64
    st_atime::Float64
    st_mtime::Float64
    st_ctime::Float64
end

# Columns of one record batch. `code` is omitted: it is constant and already in
# the file name.
function newcolumns(n::Int)
    cols = (
        name = String[], path = String[], st_size = Int64[],
        st_atime = Float64[], st_mtime = Float64[], st_ctime = Float64[]
    )
    foreach(c -> sizehint!(c, n), cols)
    return cols
end

const Columns = typeof(newcolumns(0))

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

struct Sinks
    dir::String
    lock::ReentrantLock
    files::Dict{Union{Int,Nothing},IOStream}
    counts::Dict{Union{Int,Nothing},Int}
end
Sinks(dir) = Sinks(
    dir, ReentrantLock(), Dict{Union{Int,Nothing},IOStream}(),
    Dict{Union{Int,Nothing},Int}()
)
sinkname(code) = code === nothing ? "unparsed.jsonl" : "code_$(code).jsonl"

# Rare path, so one lock is fine. Lines from different chunks interleave; sort
# later if you care.
function stash!(s::Sinks, data::Vector{UInt8}, lo::Int, hi::Int, code)
    lock(s.lock) do
        io = get!(
            () -> open(joinpath(s.dir, sinkname(code)), "w"), s.files, code
        )
        GC.@preserve data unsafe_write(io, pointer(data, lo), hi - lo + 1)
        write(io, '\n')
        s.counts[code] = get(s.counts, code, 0) + 1
    end
end

# ---- line handling -------------------------------------------------------------------------------

function handle_line!(
        cols::Columns, sinks::Sinks, data::Vector{UInt8}, lo::Int, hi::Int
    )
    line = view(data, lo:hi)
    code = try
        JSON.lazy(line).code[] # navigate to "code", materialize only that value
    catch
        nothing                # no such key / malformed → unparsed.jsonl
    end
    if code == 0
        try
            pushentry!(cols, JSON.parse(line, Entry0))
        catch
            stash!(sinks, data, lo, hi, nothing)
        end
    else
        stash!(sinks, data, lo, hi, code isa Integer ? Int(code) : nothing)
    end
end

# Parse one chunk (which ends on a '\n' or EOF) into a Columns. Runs on a
# worker thread.
function process_chunk(data::Vector{UInt8}, r::UnitRange{Int}, sinks::Sinks)
    lo, hi = first(r), last(r)
    cols = newcolumns(length(r) ÷ 150) # lines are ~190 bytes; over-estimating is harmless
    pos = lo
    while pos <= hi
        nl   = findnext(==(UInt8('\n')), data, pos)      # memchr
        stop = (nl === nothing || nl > hi) ? hi : nl - 1 # last byte of the line, excl. '\n'
        e = stop
        while e >= pos && (data[e] == UInt8('\r') || data[e] == UInt8(' ')); e -= 1; end
        e >= pos && handle_line!(cols, sinks, data, pos, e) # skips blank lines
        pos = stop + 2
    end
    return cols
end

# Split 1:length(data) into ~chunk_bytes pieces, each ending on a '\n' (or EOF).
function chunk_ranges(data::Vector{UInt8}, chunk_bytes::Int)
    n, lo, ranges = length(data), 1, UnitRange{Int}[]
    while lo <= n
        hi = min(lo + chunk_bytes - 1, n)
        if hi < n
            nl = findnext(==(UInt8('\n')), data, hi)
            hi = nl === nothing ? n : nl
        end
        push!(ranges, lo:hi)
        lo = hi + 1
    end
    return ranges
end

# ---- driver ---------------------------------------------------------------------------------------

function convert_file(infile::AbstractString, outdir::AbstractString)
    mkpath(outdir)
    data     = Mmap.mmap(infile)::Vector{UInt8}
    ranges   = chunk_ranges(data, CHUNK_BYTES)
    nchunks  = length(ranges)
    nworkers = Threads.nthreads()
    sinks    = Sinks(outdir)
    @info "converting" infile GB=round(length(data) / 1e9; digits=2) nchunks nworkers

    jobs    = Channel{Int}(nchunks)
    results = Channel{Tuple{Int,Columns}}(nworkers)
    foreach(i -> put!(jobs, i), 1:nchunks); close(jobs)

    workers = map(1:nworkers) do _
        Threads.@spawn try
            for i in jobs
                put!(results, (i, process_chunk(data, ranges[i], sinks)))
            end
        catch e
            close(results, e)   # fail the consumer instead of deadlocking it
            rethrow()
        end
    end

    # Re-sequence results so the Arrow file preserves input order. Each Columns
    # becomes one record batch; Arrow.write pulls from this channel, so only a
    # few chunks are ever held in memory.
    nrows, t0 = Ref(0), time()
    ordered = Channel{Columns}(2) do out
        pending = Dict{Int,Columns}()
        for next in 1:nchunks
            while !haskey(pending, next)
                i, cols = take!(results)
                pending[i] = cols
            end
            cols = pop!(pending, next)
            nrows[] += length(cols.name)
            put!(out, cols)
            done = last(ranges[next])
            @printf(
                stderr, "\r%5.1f%%  %6.2f GB  %5.0f MB/s ",
                100done / length(data), done / 1e9, done / 1e6 / (time() - t0)
            )
        end
    end

    Arrow.write(
        joinpath(outdir, "code_0.arrow"), Tables.partitioner(ordered);
        compress=COMPRESS
    )

    foreach(wait, workers)
    foreach(close, values(sinks.files))

    println(stderr)
    @info "done" rows_code0=nrows[] other_codes=sinks.counts seconds=round(time() - t0; digits=1)
end

function (@main)(args)
    if length(args) != 2
        println(stderr, "usage: ProcessStat input.jsonl outdir")
        return 1
    end
    convert_file(args[1], args[2])
    return 0
end

end # module ProcessStat
