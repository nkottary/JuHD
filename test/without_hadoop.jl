using DataFrames
using Base.Test

include(joinpath(Pkg.dir("DJoin"), "src", "DJoin.jl"))

const NUMPROCS = 2
const BASIC_KEYCOL = :carid
const BIG_KEYCOL = :movieId

const LOCALHOST = HadoopCtx("localhost", 9000, 8032)
const JD = HadoopCtx("ip-10-11-191-51.ec2.internal", 8020,
                      "ip-10-5-176-239.ec2.internal", 8050)

const BASIC_TEST_PATH = joinpath(Pkg.dir("DJoin"), "test", "basic_test")
const BIG_TEST_PATH = joinpath(Pkg.dir("DJoin"), "test", "big_test")

const BASIC_LEFT = joinpath(BASIC_TEST_PATH, "left.csv")
const BASIC_RIGHT = joinpath(BASIC_TEST_PATH, "right.csv")
const BASIC_JOIN = joinpath(BASIC_TEST_PATH, "join.csv")

const BIG_LEFT = joinpath(BIG_TEST_PATH, "left.csv")
const BIG_RIGHT = joinpath(BIG_TEST_PATH, "right.csv")
const BIG_JOIN = joinpath(BIG_TEST_PATH, "join.csv")

# Correctness test.
function basic_test()
    println("LOG: Time taken in init workers:")
    @time init_remote_workers(BASIC_LEFT, BASIC_RIGHT, keycol=BASIC_KEYCOL)
    println("LOG: Init workers done.")
    refs = djoin()
    df = accumulate(refs)
    dfbase = join(readtable(BASIC_LEFT), readtable(BASIC_RIGHT),
                  on=BASIC_KEYCOL, kind=:inner)
    sort!(df)
    sort!(dfbase)
    @test df == dfbase
end

# Scale test.
function big_test()
    println("LOG: Time taken in init workers:")
    @time init_remote_workers(BIG_LEFT, BIG_RIGHT, keycol=BIG_KEYCOL)
    println("LOG: Init workers done.")
    djoin()
end

# Serial join scale time.
serial_join() = join(readtable(BIG_LEFT), readtable(BIG_RIGHT),
                     on=BIG_KEYCOL, kind=:inner)

function argedtest(args)
    addworkers(args)
    
    println("\n*** TEST: Running basic test. ***\n")
    basic_test()
    println("\n*** TEST: Basic test passed. ***\n")

    println("\n*** TEST: Running big test. ***\n")
    @time refs = big_test()
    writefile(accumulate(refs), BIG_JOIN)
    println("\n*** TEST: Big test passed. ***\n")

    println("*** Time taken in serial join: ")
    @time serial_join()
    rmworkers()
end

# Test with addprocs() on local machine
function local_test()
    println("--------------------------------")
    println("|          Local Test          |")
    println("--------------------------------")
    argedtest(NUMPROCS)
end

# Test with addprocs() on cluster
function dist_test()
    println("--------------------------------")
    println("|         Remote Test          |")
    println("--------------------------------")
    argedtest(JD)
end

function without_hadoop()
    local_test()
    println("\n\n")    
    #dist_test()
end
