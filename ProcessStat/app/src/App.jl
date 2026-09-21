module App

using ProcessStat


function julia_main()::Cint
    if length(ARGS) != 2
        println(stderr,
            "usage: $(basename(PROGRAM_FILE)) input.jsonl outdir"
        )
        println(stderr,
            "Hint: For multi-threading use: `--julia-args -t<number of threads>`"
        )
        return 1
    end
    try
        ProcessStat.convert_file(ARGS[1], ARGS[2])
    catch
        Base.invokelatest(Base.display_error, Base.catch_stack())
        return 1
    end
    return 0
end

end # module App
