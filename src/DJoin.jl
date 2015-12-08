# Given two CSV files, left.csv and right.csv, read them as dataframes and
# pass chunks of each to the workers through a function call.  We also a pass
# a keyhash generated by the name node.  The workers decide what parts of the
# chunk stay with them and what parts are passed to the other workers.  Once
# this is calculated and all workers are synchronised, the data movement begins.
# 
# We can either push or pull the dataframes from one node to the other nodes.
# Each node asynchronously accumulates these dataframes.
# 
# Now we can call join asynchronously on each node, this is the `map` part.
# We then collect all the joined dataframes from each worker node to the name node
# and concatenate (vcat) them, this is the `reduce` part.

using Elly
using DataFrames
using Blocks

"""
Read only the key column from CSV and return it as an array.
"""
function readkeys(filename, keycol::Symbol, delim=',')
    # Need to handle the case when `delim` is quoted.
    f = open(filename, "r")
    headers = split(strip(readline(f)), delim)
    keyidx = findfirst(headers, string(keycol))
    if keyidx == 0
        error("Invalid `keycol` argument. The specified column name `$keycol` does not exist in this table.")
    end
    keys = Any[]
    for ln in eachline(f)
        vals = split(strip(ln), delim)
        push!(keys, parse(vals[keyidx]))
    end
    close(f)
    return keys
end

"""
Get a unique set of keys from an array of keys `keycol`.
"""
function get_uniquekeys(keycol, pool=Set())
    return union(pool, Set(keycol))
end

"""

Get pool of keys. `left_file` and `right_file` are the two
 CSV files representing the table, and `keycol` is the column representing
 the keys.
"""
function get_keypool(leftkeys, rightkeys)
    pool = get_uniquekeys(leftkeys)
    pool = get_uniquekeys(rightkeys, pool)
    return pool
end

"""
Get a hash of {key => processor_id} from `keypool`.
"""
function get_keyhash(keypool)
    parts = length(g_wids)
    hash = Dict()
    arr = collect(keypool)
    len = length(arr)
    bucketsize = div(len, parts)
    remainder = rem(len, parts)
    psize = bucketsize*parts
    for i = 1:psize
        hash[arr[i]] = g_wids[div(i-1, bucketsize) + 1]
    end
    for i = 1:remainder
        hash[arr[psize + i]] = g_wids[i]
    end
    return hash
end

"""
Partition a dataframe `df` among processes.

Returns an dictionary of process_id => dataframe.
"""
function partition_df(df)
    n = length(g_wids)
    ret = Dict()
    dsize = size(df, 1)
    psize = div(dsize, n)
    start = 1
    finish = psize 
    for i = 1:n-1
        ret[g_wids[i]] = df[start:finish, :]
        start = finish + 1
        finish = finish + psize
    end
    ret[g_wids[n]] = df[start:end, :]
    return ret
end

type HadoopCtx
    dfsurl::AbstractString
    dfsport::Int
    yarnurl::AbstractString
    yarnport::Int
    nprocs::Int        # Number of machines to use.  If this is set to
                       # 0 then all mahines are used.

    HadoopCtx(dfsurl, dfsport, yarnurl, yarnport) =
        new(dfsurl, dfsport, yarnurl, yarnport, 0)

    HadoopCtx(dfsurl, dfsport, yarnurl, yarnport, nprocs) =
        new(dfsurl, dfsport, yarnurl, yarnport, nprocs)

    HadoopCtx(url, dfsport, yarnport) =
        new(url, dfsport, url, yarnport, 0)
end

const AWS = HadoopCtx("nn.juliahub.com", 8020, 8032)
const LOCALHOST = HadoopCtx("localhost", 9000, 8032)
const JD = HadoopCtx("ip-10-11-191-51.ec2.internal", 8020,
                     "ip-10-5-176-239.ec2.internal", 8050)

const JD2 = HadoopCtx("ip-10-11-191-51.ec2.internal", 8020,
                     "ip-10-5-176-239.ec2.internal", 8050, 2)

"""
Add julia processes on remote machines in case a Hadoop context is passed
 as argument, else add julia processes on local machine.

If the `nprocs` field of context is set to 0, then all machines returned by
 yarn are used as workers and nprocs is set to the number of workers.

Returns `nothing`
"""
function addworkers(ctx::HadoopCtx)
    dfs = HDFSClient(ctx.dfsurl, ctx.dfsport)
    ugi = UserGroupInformation()
    yarncli = YarnClient(ctx.yarnurl, ctx.yarnport, ugi)
    nlist = nodes(yarncli)
    keys = nlist.status.keys
    slots = nlist.status.slots
    machines = ASCIIString[]
    proccount = 0
    for i = 1:length(keys)
        if slots[i] == 1
            push!(machines, keys[i].host)
            proccount += 1
            proccount == ctx.nprocs && break
        end
    end
    if ctx.nprocs == 0; ctx.nprocs = proccount; end
    global g_wids = addprocs(machines)
    #@everywhere include(joinpath(Pkg.dir("DJoin"), "src", "WorkerDefs.jl"))
    return nothing
end

function addworkers(num)
    global g_wids = addprocs(num)
    @everywhere include(joinpath(Pkg.dir("DJoin"), "src", "WorkerDefs.jl"))
    return nothing
end

rmworkers() = rmprocs(g_wids...)

# Get a dict where key is the processor id and value is the tuple (filename, range).
# The tuple is an element in the block array field of the Block type.
chunkdict(filename) = Dict(zip(g_wids, Block(Base.FS.File(filename)).block))

# Get the headers as an array of symbols from a csv file
getheaders(filename) = open(filename) do f
    map(x->Symbol(strip(x)), split(readline(f), ","))
end

g_debug = false
debug_print(str...) = g_debug == true && println(str...)

function initallworkers(leftfn, rightfn, keycol)
    leftheaders = getheaders(leftfn)
    rightheaders = getheaders(rightfn)

    # Get the dicts of proc_id => chunk (range).
    leftchunks = chunkdict(leftfn)
    rightchunks = chunkdict(rightfn)

    @sync for wid in g_wids
        @async remotecall_fetch(wid, initworkerctx, leftchunks[wid],
                                rightchunks[wid], leftheaders, rightheaders,
                                keycol, g_wids)
    end
end

function getfairkeyhash()
    refs = Dict()
    @sync for wid in g_wids
        @async begin 
            ref = remotecall(wid, getkeycount)
            refs[wid] = ref
        end
    end
    keycount = Dict()   # A dict of (worker_id => kcdict), where kcdict
                        # is a dict of (key => count).
    keyset = Set()      # A list of unique keys.
    for (wid, ref) in refs
        kc = fetch(ref)
        keycount[wid] = kc
        keyset = union(keyset, keys(kc))
    end
    keyhash = Dict()    # The final (key => worker_id) dict to return.
    dist = Dict()       # A dict of (worker_id => number_of_rows_alloted).
    for wid in g_wids
        dist[wid] = 0
    end
    for key in keyset
        maxcount = 0    # The maximum count of this key.
        maxwids = Int[] # The worker id's for which this key
                        # occurs maxcount number of times.
        keysum = 0      # The number of times this key occurs in all workers.
        for wid in g_wids
            if haskey(keycount[wid], key)
                val = keycount[wid][key]
                if val > maxcount
                    maxcount = val
                    maxwids = [wid]
                elseif val == maxcount
                    push!(maxwids, wid)
                end
                keysum += val
            end
        end
        # *** Is this a variant of knapsack?
        widcounts = [dist[wid] for wid in maxwids]
        chosen_wid = maxwids[indmin(widcounts)]
        keyhash[key] = chosen_wid
        dist[chosen_wid] += keysum  # Accumulate number of rows alloted to this processor.
    end
    return keyhash
end

function initalldistribution(keyhash)
    @sync for wid in g_wids
        @async remotecall_fetch(wid, initdistribution, keyhash)
    end
end

function communicate()
    @sync for wid in g_wids
        @async remotecall_fetch(wid, communicate_push)
    end
end

function mapjoin(keycol, kind)
    refs = RemoteRef[]
    @sync for wid in g_wids
        @async begin
            ref = remotecall(wid, joindf, keycol, kind)
            push!(refs, ref)
        end
    end
    return refs
end

function reducedf(refs)
    df = DataFrame()
    for ref in refs
        df = vcat(df, fetch(ref))
    end
    return df
end

"""
Main function that performs the distributed join.
 `leftfn` and `rightfn` are the left and right table csv filename.
 `opfn` is the output file name. `keycol` is the column symbol on which to join.
 `kind` is the type of join i.e, `:inner`, `:outer` etc.
"""
function djoin(leftfn, rightfn, opfn; keycol=nothing, kind=nothing)
    keycol == nothing && throw(ArgumentError("Missing join argument `keycol`."))
    kind == nothing && throw(ArgumentError("Missing join argument `kind`."))

    println("Time consumed in init workers: ")
    @time initallworkers(leftfn, rightfn, keycol)
    debug_print("LOG: Done Initializing workers.")

    println("Time consumed in generating keyhash: ")
    @time keyhash = getfairkeyhash()
    debug_print("LOG: Getting keyhash done.")

    println("Time consumed in init distribution: ")
    @time initalldistribution(keyhash)
    debug_print("LOG: Done Initializing distribution.")

    println("Time consumed in communicate: ")
    @time communicate()
    debug_print("LOG: Data Movement done.")

    # call join and get an array of refs
    println("Time consumed in map: ")
    @time refs = mapjoin(keycol, kind)
    debug_print("LOG: Done calling join on each worker.")

    # fetch and concatenate
    println("Time consumed in vcat: ")
    @time df = reducedf(refs)
    debug_print("LOG: Done accumulating dataframes.")

    println("Time consumed in writetable: ")
    @time writetable(opfn, df)
    debug_print("LOG: done writetable")

    # Uncomment to log the context state of each process
    # for i = 1:np
    #     remotecall(i, logdata)
    # end
    return df
end
