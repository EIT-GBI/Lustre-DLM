using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using PackageCompiler
create_app(
    joinpath(@__DIR__, ".."),
    ARGS[1],
    executables = ["ProcessStat" => "julia_main"]
)
