using DataFrames
using Base.Test
using Elly

include(joinpath(Pkg.dir("DJoin"), "src", "DJoin.jl"))

const NUMPROCS = 2
const BIG_KEYCOL = :movieId

const LOCALHOST = HadoopCtx("localhost", 9000, 8032)
const JD = HadoopCtx("ip-10-11-191-51.ec2.internal", 8020,
                      "ip-10-5-176-239.ec2.internal", 8050)

const DIR_PATH = "user/mapred/julia/djoin"
const BIG_TEST_PATH = joinpath(DIR_PATH, "big_test")

const BIG_LEFT = HDFSFile(joinpath(BIG_TEST_PATH, "left.csv"))
const BIG_RIGHT = HDFSFile(joinpath(BIG_TEST_PATH, "right.csv"))
const BIG_JOIN_FOLDER = HDFSFile(joinpath(BIG_TEST_PATH, "join"))

# Scale test.
function big_test()
    println("LOG: Time taken in init workers:")
    left = HDFSFile(BIG_LEFT)
    right = HDFSFile(BIG_RIGHT)
    @time init_remote_workers(left, right, keycol=BIG_KEYCOL)
    println("LOG: Init workers done.")
    djoin()
end

function argedtest(args)
    addworkers(args)
    
    println("\n*** TEST: Running big test. ***\n")
    @time refs = big_test()
    mapwrite(BIG_JOIN_FOLDER)
    println("\n*** TEST: Big test passed. ***\n")

    rmworkers()
end

function dist_test()
    println("--------------------------------")
    println("|         Remote Test          |")
    println("--------------------------------")
    argedtest(JD)
end

function with_hadoop()
    println("\n\n")    
    dist_test()
end
