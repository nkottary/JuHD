using DataFrames
using Base.Test

include(joinpath(Pkg.dir("DJoin"), "src", "DJoin.jl"))

const NUMPROCS = 2

const AWS = HadoopCtx("nn.juliahub.com", 8020, 8032)
const LOCALHOST = HadoopCtx("localhost", 9000, 8032)
const JD = HadoopCtx("ip-10-11-191-51.ec2.internal", 8020,
                      "ip-10-5-176-239.ec2.internal", 8050)

const BASIC_TEST_PATH = joinpath(Pkg.dir("DJoin"), "test", "basic_test")
const BIG_TEST_PATH = joinpath(Pkg.dir("DJoin"), "test", "big_test")

const BASIC_LEFT = joinpath(BASIC_TEST_PATH, "left.csv")
const BASIC_RIGHT = joinpath(BASIC_TEST_PATH, "right.csv")
const BASIC_JOIN = joinpath(BASIC_TEST_PATH, "join.csv")
const BASIC_BASE = joinpath(BASIC_TEST_PATH, "join_base.csv")

const BIG_LEFT = joinpath(BIG_TEST_PATH, "left.csv")
const BIG_RIGHT = joinpath(BIG_TEST_PATH, "right.csv")
const BIG_JOIN = joinpath(BIG_TEST_PATH, "join.csv")

function cleanup()
    try
        rm(BASIC_JOIN)
        rm(BIG_JOIN)
    end
end

# Correctness test.
function basic_test()
    djoin(BASIC_LEFT, BASIC_RIGHT, BASIC_JOIN, keycol=:carid, kind=:inner)
    df = readtable(BASIC_JOIN)
    dfbase = readtable(BASIC_BASE)
    sort!(df)
    sort!(dfbase)
    @test df == dfbase
end

# Scale test.
big_test() = @time djoin(BIG_LEFT, BIG_RIGHT, BIG_JOIN, keycol=:movieId, kind=:inner)

# Serial join scale time.
serial_join() = @time join(readtable(BIG_LEFT), readtable(BIG_RIGHT),
                           on=:movieId, kind=:inner)

function argedtest(args)
    addworkers(args)
    cleanup()
    
    println("\n*** TEST: Running basic test. ***\n")
    basic_test()
    println("\n*** TEST: Basic test passed. ***\n")

    println("\n*** TEST: Running big test. ***\n")
    big_test()
    println("\n*** TEST: Big test passed. ***\n")

    println("*** Time taken in serial join: ")
    serial_join()
    rmworkers()
end

# Test with addprocs() on local machine
local_test() = argedtest(NUMPROCS)

# Test with addprocs() on cluster
dist_test() = argedtest(JD)

function main()
    println("--------------------------------")
    println("|          Local Test          |")
    println("--------------------------------")
    local_test()
    
    println("\n\n")
    
    println("--------------------------------")
    println("|         Remote Test          |")
    println("--------------------------------")
    dist_test()
end

main()
