@everywhere using DataFrames

# TODO: Assuming the key pool is within 10 MB. The key pool generation is not
# out of core. However, loading the keys from CSV would mean loading rows from
# CSV and this would have to be out of core, unless we write a custom readcsv
# that only reads the key column.

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
Get a hash of {key => processor_id} from `keypool` and `parts` number of parts.
"""
function get_keyhash(keypool, parts)
    hash = Dict()
    arr = collect(keypool)
    len = length(arr)
    bucketsize = div(len, parts)
    remainder = rem(len, parts)
    psize = bucketsize*parts
    for i = 1:psize
        hash[arr[i]] = div(i-1, bucketsize) + 1
    end
    for i = 1:remainder
        hash[arr[psize + i]] = i
    end
    return hash
end


"""
Partition a dataframe `df` into `n` parts.

Returns an array of dataframes.
"""
function partition_df(df, n)
    ret = Array(DataFrame, n)
    dsize = size(df, 1)
    psize = div(dsize, n)
    start = 1
    finish = psize 
    for i = 1:n-1
        ret[i] = df[start:finish, :]
        start = finish + 1
        finish = finish + psize
    end
    ret[n] = df[start:end, :]
    return ret
end

# @everywhere definitions start here

@everywhere type ProcessCtx
    dfarr_left::Array{DataFrame, 1}
    dfarr_right::Array{DataFrame, 1}
    accdf_left::DataFrame
    accdf_right::DataFrame
end

@everywhere function Base.show(io::IO, ctx::ProcessCtx)
    procid = myid()
    print(io, "\n=================\n\nProcess: $procid\n\nDF array left: $(ctx.dfarr_left)\n\nDF array right: $(ctx.dfarr_right)\n\nAcc DF left: $(ctx.accdf_left)\n\nAcc DF right: $(ctx.accdf_right)\n===================\n")
end

# Determine which of the keys in `keyarr` belongs to which process from the
#  hash of keys and process id's `keyhash`. `parts` is the number of processors.
# 
# Returns an array of indexes of the dataframe. The array at index `i` of the
#  return value represents the `i`th processors indexes.
@everywhere function arrangement_idxs(keyarr, keyhash, parts)
    idxs = Array(Array, parts)
    for i in 1:parts
        idxs[i] = Int[]
    end
    for i = 1:length(keyarr)
        procid = keyhash[keyarr[i]]
        push!(idxs[procid], i)
    end

    return idxs
end

# Get an array of sub-dataframes from the dataframe `df` and array of indexes
# `idxs`
@everywhere function idxs_to_dfarray(df, idxs)
    np = length(idxs)
    dfarr = Array(DataFrame, np)
    for i = 1:np
        dfarr[i] = df[idxs[i], :]
    end
    return dfarr
end

# Figure out what parts of the received dataframe should be given to what
# process. Store this in a global context. Also initialize the accumulator
# dataframe in the global context.
@everywhere function init_process(dfl, dfr, keyhash, keycol, np)
    procid = myid()
    idxsl = arrangement_idxs(dfl[keycol], keyhash, np)
    idxsr = arrangement_idxs(dfr[keycol], keyhash, np)

    dfarr_left = idxs_to_dfarray(dfl, idxsl)
    dfarr_right = idxs_to_dfarray(dfr, idxsr)

    accdf_left = dfarr_left[procid]      # df belonging to self
    accdf_right = dfarr_right[procid]

    global g_ctx = ProcessCtx(dfarr_left, dfarr_right, accdf_left, accdf_right)
end

# Get the `typ` dataframe belonging to process `pid`.
@everywhere function getrows(pid, typ)
    if typ == :left
        return g_ctx.dfarr_left[pid]
    else
        return g_ctx.dfarr_right[pid]
    end
end

# Pull rows of `typ` dataframe belonging to this process from the other processes.
# `typ` is :left or :right. 
@everywhere function pullrows(typ)
    np = nprocs()
    procid = myid()
    refs = RemoteRef[]
    @sync for j = 1:np
        if j == procid
            continue
        end
        @async begin
            ref = remotecall(j, getrows, procid, typ)
            push!(refs, ref)
        end
    end
    for ref in refs
        df = fetch(ref)
        if size(df, 1) == 0
            continue
        end
        if typ == :left
            g_ctx.accdf_left = vcat(g_ctx.accdf_left, df)
        else
            g_ctx.accdf_right = vcat(g_ctx.accdf_right, df)
        end
    end
end

# Send the sub-dataframe's in the array of dataframes `dfarr` to
# the respective processes. `typ` which is either `:left` or `:right`
# tells the receiving process wether this is to be accumulated in
# the left dataframe or right dataframe.
@everywhere function pushrows(dfarr, typ)
    procid = myid()
    @sync for i = 1:length(dfarr)
        if (size(dfarr[i], 1) == 0) # No need to send empty df's
            continue
        end
        if (procid == i) # No need to send to self
            continue
        end
        @async remotecall(i, recvrows, dfarr[i], typ)
    end
end

# Recieve rows from other processes and concatenate them.
@everywhere function recvrows(df, typ)
    if typ == :left
        g_ctx.accdf_left = vcat(g_ctx.accdf_left, df)
    else
        g_ctx.accdf_right = vcat(g_ctx.accdf_right, df)
    end
end

# Pass sub-dataframes to respective processes.
@everywhere function communicate_push()
    pushrows(g_ctx.dfarr_left, :left)
    pushrows(g_ctx.dfarr_right, :right)
end

# Pass sub-dataframes to respective processes.
@everywhere function communicate_pull()
    pullrows(:left)
    pullrows(:right)
end

@everywhere function logdata()
    procid = myid()
    f = open("log$procid", "w")
    print(f, g_ctx)
    close(f)
end

# Join the left and right df belonging to you.
@everywhere function joindf(keycol)
    return join(g_ctx.accdf_left, g_ctx.accdf_right, on=keycol, kind=:inner)
end

function main()
    np = nprocs()
    const keycol = :carid

    # The serial part: Get the keyhash
    leftkeys = readkeys("left.csv", keycol)
    rightkeys = readkeys("right.csv", keycol)
    keypool = get_keypool(leftkeys, rightkeys)
    keyhash = get_keyhash(keypool, np)

    # Partition dataframe
    dfl = readtable("left.csv")
    dflparts = partition_df(dfl, np)
    dfr = readtable("right.csv")
    dfrparts = partition_df(dfr, np)

    # The parallel parts:
    # For each process give a copy of the keyhash and a part of the dataframes.
    @sync for i = 1:np
        @async remotecall(i, init_process, dflparts[i], dfrparts[i], keyhash,
                          keycol, np)
    end

    # Rearrange the rows of the dataframe between processes.
    for i = 1:np
        remotecall(i, communicate_pull)
    end

    sleep(2)  # The below code picks up data before the communicate step without this sleep.

    # call join and get an array of refs
    refs = RemoteRef[]
    @sync for i = 1:np
        @async begin
            ref = remotecall(i, joindf, keycol)
            push!(refs, ref)
        end
    end

    # fetch and concatenate
    df = DataFrame()
    for ref in refs
        df = vcat(df, fetch(ref))
    end

    println(df)

    # Uncomment to log the context state of each process
    #     for i = 1:np
    #         remotecall(i, logdata)
    #     end
end
