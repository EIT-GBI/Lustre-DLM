"""
    App

The command-line front end of `ProcessStat`, compiled into an executable by
`deps/build.jl`: `julia_main` parses the arguments (`USAGE` lists them) and
calls `ProcessStat.convert_file`.
"""
module App

using ProcessStat

"The help text; also the summary of the options `parse_args` understands."
const USAGE = """
usage: ProcessStat [options] input.jsonl outdir

options (each also settable via the environment variable shown; the flag wins):
  --chunk-mb N    JSONL MB per chunk = per Arrow record batch    PROCESSSTAT_CHUNK_MB  (default 8)
  --inflight N    chunks parsed+encoded at once (worker tasks)   PROCESSSTAT_INFLIGHT  (default 8)
  --window N      chunks claimed but not yet written (>= workers) PROCESSSTAT_WINDOW  (default 2 x workers)
  --encode MODE   parallel | serial (serial: Arrow.write per batch on one task, slower; for testing)  PROCESSSTAT_ENCODE
  -h, --help      show this help

Peak memory ~ 0.45 GB + 7 x workers x chunk-mb MB (defaults: ~1 GB), with workers = min(inflight, threads);
it does not grow with the file size or with the thread count beyond --inflight.
Options go before --julia-args. For multi-threading use: `--julia-args -t<number of threads>`
"""

"The integer options and the `convert_file` keyword each one sets."
const INTFLAGS = Dict(
    "--chunk-mb" => :chunk_mb,
    "--inflight" => :inflight,
    "--window"   => :window
)

"""
    parse_args(args) -> (positional, kwargs)

Split `args` into the two positional arguments (input file and outdir) and the
keyword arguments for `ProcessStat.convert_file`. Throws an `ArgumentError`
whose message is to be shown, or whose message is `"help"` for `-h`/`--help`.
"""
function parse_args(args::Vector{String})
    pos = String[]
    kw  = Dict{Symbol,Any}()
    i   = 1
    while i <= length(args)
        a = args[i]
        if a in ("-h", "--help")
            throw(ArgumentError("help"))
        elseif startswith(a, "--")
            name, val = occursin('=', a) ? split(a, '='; limit=2) : (a, nothing)
            if val === nothing
                i += 1
                i <= length(args) || throw(ArgumentError("$name needs a value"))
                val = args[i]
            end
            if haskey(INTFLAGS, name)
                v = tryparse(Int, val)
                v === nothing && throw(
                    ArgumentError("$name expects an integer, got $(repr(val))")
                )
                kw[INTFLAGS[name]] = v
            elseif name == "--encode"
                kw[:encode] = Symbol(val)
            else
                throw(ArgumentError("unknown option $name"))
            end
        elseif startswith(a, "-")
            throw(ArgumentError(
                "unknown option $a (Julia options such as -t8 go after --julia-args)"
            ))
        else
            push!(pos, a)
        end
        i += 1
    end
    length(pos) == 2 || throw(ArgumentError("expected input.jsonl and outdir"))
    return pos, (; kw...)
end

"""
    julia_main() -> Cint

Entry point of the compiled app: parse `ARGS`, run the conversion and return
the exit status, 0 on success and 1 on a usage error or a failed conversion
(which leaves `code_0.arrow.partial` behind).
"""
function julia_main()::Cint
    # With no interactive thread (-t1, JULIA_NUM_THREADS=1, -tN,0) ^C would be
    # thrown into whatever task runs on thread 1, usually a worker, where it
    # can leak an IOStream lock (hang). Exit instead; the code_0.arrow.partial
    # stays. (Not unconditionally: a ^C that lands during JIT can then deadlock
    # the exit path.)
    Threads.nthreads(:interactive) == 0 && Base.exit_on_sigint(true)

    pos, kw = try
        parse_args(ARGS)
    catch e
        help = e isa ArgumentError && e.msg == "help"
        if !help
            msg = e isa ArgumentError ? e.msg : sprint(showerror, e)
            println(stderr, "error: ", msg)
        end
        print(help ? stdout : stderr, USAGE)
        return help ? 0 : 1
    end

    try
        ProcessStat.convert_file(pos[1], pos[2]; kw...)
    catch
        Base.invokelatest(Base.display_error, Base.catch_stack())
        return 1
    end
    return 0
end

end # module App
