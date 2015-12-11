#=      Join tables on a distributed system.

        Implementation:
        1) Workers are initialized.  Workers  can be remote systems or
           local processes.  Remote systems are accesed using Elly.jl.
        2) Input tables  are given as CSV files.   Workers are alloted
           equal sized chunks using Blocks.jl.
        3) Chunk  ranges are  communicated to  each worker  along with
           table headers and  the header (`keycol`) on  which the join
           is to happen.
        3) Workers read  respective chunks as DataFrames  and return a
           dictionary  of  `(key =>  count)`  for  each `key`  in  the
           `keycol`.
        4) Based on all the key-count dictionaries returned by all the
           workers, the manager generates a key-hash.
        5) The key-hash is a dictionary of `(key => wid)`, all rows of
           the tables whose `keycol` is  `key` will be moved to worker
           `wid` .
        6) The key-hash is generated in  such a way as to minimize the
           number  of rows  moved  and  provide an  even  load on  all
           workers.
        7) The  key-hash is broadcasted  to all workers.   Each worker
           generates a dictionary of `(wid => idxs)`, where `wid` is a
           worker id and `idxs` is an  array of indexes of rows in its
           DataFrame that have to be moved to worker `wid`.
        8) The  rows are moved  to their respective workers  from each
           worker.  Each  worker accumulates the  received DataFrame's
           to obtain a single DataFrame for each table.
        9)  On  each worker  `DataFrames.join()`  is  called with  the
           resultant DataFrame's from the previous step as input.
        10) The  results of  `join` are moved  to the  manager system,
            accumulated and stored in a file.
=#

using Elly
using DataFrames
using Blocks

const WORKERDEFS_PATH = joinpath(Pkg.dir("DJoin"), "src", "WorkerDefs.jl")

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

"""
Broadcast worker definitions.  Call include with the location
 of WorkerDef.jl on all workers.
"""
function bcast_workerdefs()
    @sync for wid in g_wids
        @async remotecall_fetch(wid, include, WORKERDEFS_PATH)
    end
end

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
    @everywhere include(joinpath(Pkg.dir("DJoin"), "src", "WorkerDefs.jl"))
    # bcast_workerdefs()
end

function addworkers(num)
    global g_wids = addprocs(num)
    @everywhere include(joinpath(Pkg.dir("DJoin"), "src", "WorkerDefs.jl"))
    # bcast_workerdefs()
    return nothing
end

rmworkers() = rmprocs(g_wids...)

"""
Get a dict where key is the processor id and value is the tuple (filename, range).
 The tuple is an element in the `block` array field of the `Block` type.
"""
chunkdict(filename) = Dict(zip(g_wids, Block(Base.FS.File(filename)).block))

"""
Get the headers as an array of symbols from a csv file.
"""
getheaders(filename, delim) = open(filename) do f
    map(x->Symbol(strip(x)), split(readline(f), delim))
end

g_debug = false
debug_print(str...) = g_debug == true && println(str...)

"""
Send the table headers, the chunk ranges and `keycol`, the column on which
 join is to happen, to each worker.
"""
function init_remote_workers(leftfn, rightfn, keycol)
    leftsep = DataFrames.getseparator(leftfn)
    rightsep = DataFrames.getseparator(leftfn)
    leftheaders = getheaders(leftfn, leftsep)
    rightheaders = getheaders(rightfn, rightsep)

    # Get the dicts of proc_id => chunk (range).
    leftchunks = chunkdict(leftfn)
    rightchunks = chunkdict(rightfn)

    @sync for wid in g_wids
        @async remotecall_fetch(wid, initworkerctx, leftchunks[wid],
                                rightchunks[wid], leftheaders, rightheaders,
                                leftsep, rightsep, keycol, g_wids)
    end
end

"""
Call `getkeycount` on all workers.

Returns a dict of `(wid => ref)`.  Where `wid` is the worker id and
 `ref` is the `RemoteRef` to the key-count dictionary returned by the
  worker.
"""
function remote_keycount()
    refs = Dict()
    @sync for wid in g_wids
        @async begin 
            ref = remotecall(wid, getkeycount)
            refs[wid] = ref
        end
    end
    return refs
end

"""
Generate a key-hash, a dictionary of `(key => wid)`.  This indicates that
 rows of the tables that have `key` in their join column will be moved to
 worker with id `wid`.

The keyhash is generated in such a way as to minimize the number of rows
 moved and the distribution of rows is even among workers.
"""
function getkeyhash()
    refs = remote_keycount()
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
        widcounts = [dist[wid] for wid in maxwids]
        chosen_wid = maxwids[indmin(widcounts)]
        keyhash[key] = chosen_wid
        dist[chosen_wid] += keysum  # Accumulate number of rows alloted to this processor.
    end
    return keyhash
end

"""
Call `initdistribution` on each worker.  This will cause each worker to generate
 a dictionary of `(wid => idxs)`, where `idxs` is an array of indexes of rows
 of this workers DataFrame that is to be moved to worker with id `wid`. 
"""
function init_remote_distribution(keyhash)
    @sync for wid in g_wids
        @async remotecall_fetch(wid, initdistribution, keyhash)
    end
end

"""
Call `communicate_push` on each worker.  Each worker will send rows of its
 DataFrame to other workers based on the indexes generated using `initdistribution`.
"""
function communicate()
    @sync for wid in g_wids
        @async remotecall_fetch(wid, communicate_push)
    end
end

"""
Call `joindf` on each worker.

Returns a remote reference to the join result.
"""
function mapjoin(kind)
    refs = RemoteRef[]
    @sync for wid in g_wids
        @async begin
            ref = remotecall(wid, joindf, kind)
            push!(refs, ref)
        end
    end
    return refs
end

"""
Accumulate join results from remote references.
"""
function reducedf(refs)
    df = DataFrame()
    for ref in refs
        df = vcat(df, fetch(ref))
    end
    return df
end

"""
Call `logdata` on each worker.
"""
function log_remote_data()
    @sync for i = 1:np
        @async remotecall_fetch(i, logdata)
    end
end

"""
Preform distributed join.
 `leftfn` and `rightfn` are the left and right table csv filename.
 `keycol` is the column symbol on which to join.
 `kind` is the type of join i.e, `:inner`, `:outer` etc.

Returns to remote references to results of the join.
"""
function djoin(leftfn, rightfn; keycol=nothing, kind=nothing)
    keycol == nothing && throw(ArgumentError("Missing join argument `keycol`."))
    kind == nothing && throw(ArgumentError("Missing join argument `kind`."))

    println("Time consumed in init workers: ")
    @time init_remote_workers(leftfn, rightfn, keycol)
    debug_print("LOG: Done Initializing workers.")

    println("Time consumed in generating keyhash: ")
    @time keyhash = getkeyhash()
    debug_print("LOG: Getting keyhash done.")

    println("Time consumed in init distribution: ")
    @time init_remote_distribution(keyhash)
    debug_print("LOG: Done Initializing distribution.")

    println("Time consumed in communicate: ")
    @time communicate()
    debug_print("LOG: Data Movement done.")

    println("Time consumed in map: ")
    @time refs = mapjoin(kind)
    debug_print("LOG: Done calling join on each worker.")

    return refs
end

function accumulate(refs)
    println("Time consumed in vcat: ")
    @time df = reducedf(refs)
    debug_print("LOG: Done accumulating dataframes.")

    return df
end

function writefile(df, opfn)
    println("Time consumed in writetable: ")
    @time writetable(opfn, df)
    debug_print("LOG: done writetable")
end
