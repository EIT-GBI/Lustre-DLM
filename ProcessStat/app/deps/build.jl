# usage: julia app/deps/build.jl [outdir]
# Without an argument the app goes to ProcessStat/build, wherever this is run
# from, so that there is only ever one compiled ProcessStat. An explicit outdir
# is relative to the current directory. An existing outdir is replaced only if
# it is (the remains of) a ProcessStat build; anything else is an error, never
# deleted.
using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using PackageCompiler
outdir = isempty(ARGS) ? normpath(joinpath(@__DIR__, "..", "..", "build")) : abspath(ARGS[1])

const APPDIRS = ["bin", "lib", "libexec", "share"]
# unmistakably a finished app of this script (not e.g. a prefix like ~/.local
# with bin/ProcessStat)
isbuild(d) = isfile(joinpath(d, "lib", "julia", "sys." * Base.Libc.Libdl.dlext))                     &&
             isfile(joinpath(d, "bin", "ProcessStat")) && !islink(joinpath(d, "bin", "ProcessStat")) &&
             readdir(d) ⊆ APPDIRS && readdir(joinpath(d, "bin")) ⊆ ["ProcessStat", "julia"]          &&
             isdir(joinpath(d, "share")) && readdir(joinpath(d, "share")) ⊆ ["julia"]
# the default ProcessStat/build (gitignored) belongs to this script, also when
# a build was cut short
force = isdir(outdir) && (
    isempty(ARGS) ? readdir(outdir) ⊆ APPDIRS : isbuild(outdir)
)
isdir(outdir) && !force && error(
    "$outdir exists and is not a ProcessStat build; remove it or pick another outdir"
)

create_app(
    joinpath(@__DIR__, ".."),
    outdir,
    executables = ["ProcessStat" => "julia_main"],
    force = force,
)
@info "built" executable=joinpath(outdir, "bin", "ProcessStat")
