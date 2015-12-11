using DataFrames
using Base.Test

include(joinpath(Pkg.dir("DJoin"), "src", "DJoin.jl"))

const JD = HadoopCtx("ip-10-11-191-51.ec2.internal", 8020,
                      "ip-10-5-176-239.ec2.internal", 8050)

const JD_TEST_PATH = joinpath(Pkg.dir("DJoin"), "test", "jd_test")

const JD_LEFT = joinpath(JD_TEST_PATH, "left.tsv")
const JD_RIGHT = joinpath(JD_TEST_PATH, "right.tsv")
const JD_JOIN = joinpath(JD_TEST_PATH, "join.tsv")
const KEYCOL = :fid

jd_test() = djoin(JD_LEFT, JD_RIGHT, keycol=KEYCOL, kind=:inner)

# Serial join scale time.
function serial_join()
    left = readtable(JD_LEFT)
    right = readtable(JD_RIGHT)
    return join(left, right, on=KEYCOL, kind=:inner)
end

function argedtest(args)
    addworkers(args)
    
    println("\n*** TEST: Running jd test. ***\n")
    @time refs = jd_test()
    writefile(accumulate(refs), JD_JOIN)
    println("\n*** TEST: Jd test passed. ***\n")

    println("*** Time taken in serial join: ")
    @time serial_join()
    rmworkers()
end

# Test with addprocs() on cluster
function main()
    println("--------------------------------")
    println("|         Remote Test          |")
    println("--------------------------------")
    argedtest(2)
    #argedtest(JD)
end
