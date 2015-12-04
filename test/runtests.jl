using DataFrames
using Base.Test

include(joinpath(Pkg.dir("DJoin"), "src", "DJoin.jl"))

const NUMPROCS = 2

const BASIC_TEST_PATH = joinpath(Pkg.dir("DJoin"), "test", "basic_test")
const BIG_TEST_PATH = joinpath(Pkg.dir("DJoin"), "test", "big_test")

const BASIC_LEFT = joinpath(BASIC_TEST_PATH, "left.csv")
const BASIC_RIGHT = joinpath(BASIC_TEST_PATH, "right.csv")
const BASIC_JOIN = joinpath(BASIC_TEST_PATH, "join.csv")
const BASIC_BASE = joinpath(BASIC_TEST_PATH, "join_base.csv")

const BIG_LEFT = joinpath(BIG_TEST_PATH, "left.csv")
const BIG_RIGHT = joinpath(BIG_TEST_PATH, "right.csv")
const BIG_JOIN = joinpath(BIG_TEST_PATH, "join.csv")
const BIG_BASE = joinpath(BIG_TEST_PATH, "join_base.csv")

function cleanup()
    try
        rm(BASIC_JOIN)
        rm(BIG_JOIN)
    end
end

function basic_test()
    djoin(BASIC_LEFT, BASIC_RIGHT, BASIC_JOIN, keycol=:carid, kind=:inner)
    df = readtable(BASIC_JOIN)
    dfbase = readtable(BASIC_BASE)
    sort!(df)
    sort!(dfbase)
    @test df == dfbase
end

function serial_join()
    println("Serial Big join time")
    @time join(readtable(BIG_LEFT), readtable(BIG_RIGHT), on=:movieId, kind=:inner)
end

function big_test()
    @time djoin(BIG_LEFT, BIG_RIGHT, BIG_JOIN, keycol=:movieId, kind=:inner)
    # df = readtable(BIG_JOIN)
    # dfbase = readtable(BIG_BASE)
    # sort!(df)
    # sort!(dfbase)
    # @test df == dfbase
end

initworkers(NUMPROCS)
cleanup()
basic_test()
println("*** TEST: Basic test passed.")
big_test()
println("*** TEST: Big test passed.")
serial_join()
cleanup()
cleanupworkers()
