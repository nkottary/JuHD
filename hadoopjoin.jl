# Given two CSV files, left.csv and right.csv, read them as dataframes and
# pass chunks of each to the slaves through a function call.  We also a pass
# a keyhash generated by the name node.  The slaves decide what parts of the
# chunk stay with them and what parts are passed to the other slaves.  Once
# this is calculated and all slaves are synchronised, the data movement begins.
# 
# We can either push or pull the dataframes from one node to the other nodes.
# Each node asynchronously accumulates these dataframes.
# 
# Now we can call join asynchronously on each node, this is the `map` part.
# We then collect all the joined dataframes from each slave node to the name node
# and concatenate (vcat) them, this is the `reduce` part.

using Elly
using DataFrames

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
    keys = AbstractString[]
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
    parts = length(g_sids)
    hash = Dict()
    arr = collect(keypool)
    len = length(arr)
    bucketsize = div(len, parts)
    remainder = rem(len, parts)
    psize = bucketsize*parts
    for i = 1:psize
        hash[arr[i]] = g_sids[div(i-1, bucketsize) + 1]
    end
    for i = 1:remainder
        hash[arr[psize + i]] = g_sids[i]
    end
    return hash
end

"""
Partition a dataframe `df` among processes.

Returns an dictionary of process_id => dataframe.
"""
function partition_df(df)
    n = length(g_sids)
    ret = Dict()
    dsize = size(df, 1)
    psize = div(dsize, n)
    start = 1
    finish = psize 
    for i = 1:n-1
        ret[g_sids[i]] = df[start:finish, :]
        start = finish + 1
        finish = finish + psize
    end
    ret[g_sids[n]] = df[start:end, :]
    return ret
end

"""
Initialize julia processes on the slave nodes.
"""
function initslaves()
    dfs = HDFSClient("nn.juliahub.com", 8020)
    ugi = UserGroupInformation()
    yarncli = YarnClient("nn.juliahub.com", 8032, ugi)
    nlist = nodes(yarncli)
    keys = nlist.status.keys
    slots = nlist.status.slots
    machines = ASCIIString[]
    for i = 1:length(keys)
        if slots[i] == 1
            push!(machines, keys[i].host)
        end
    end
    # start processes on the slaves.
    return addprocs(machines)
end

function main()
    global g_sids
    g_sids = initslaves()
    np = length(g_sids)
    const keycol = :carid

    # The serial part: Get the keyhash
    leftkeys = readkeys("/home/ubuntu/JD_join/JuHD/left.csv", keycol)
    rightkeys = readkeys("/home/ubuntu/JD_join/JuHD/right.csv", keycol)
    keypool = get_keypool(leftkeys, rightkeys)
    keyhash = get_keyhash(keypool)

    # Partition dataframe
    dfl = readtable("/home/ubuntu/JD_join/JuHD/left.csv")
    dflparts = partition_df(dfl)
    dfr = readtable("/home/ubuntu/JD_join/JuHD/right.csv")
    dfrparts = partition_df(dfr)

    # The parallel parts:
    # For each process give a copy of the keyhash and a part of the dataframes.
    @everywhere include("/home/ubuntu/JD_join/JuHD/slavedefs.jl")
    @sync for sid in g_sids
        @async remotecall(sid, initprocess, dflparts[sid], dfrparts[sid],
                          keyhash, keycol, g_sids)
    end

    sleep(2)

    # Rearrange the rows of the dataframe between processes.
    for sid in g_sids
        remotecall(sid, communicate_push)
    end

    sleep(2)

    # call join and get an array of refs
    refs = RemoteRef[]
    @sync for sid in g_sids
        @async begin
            ref = remotecall(sid, joindf, keycol)
            push!(refs, ref)
        end
    end

    # fetch and concatenate
    df = DataFrame()
    for ref in refs
        df = vcat(df, fetch(ref))
    end

    println(df)

    rmprocs(g_sids...)
    return 0
    # Uncomment to log the context state of each process
    # for i = 1:np
    #     remotecall(i, logdata)
    # end
end
