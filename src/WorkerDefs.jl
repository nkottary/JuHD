# Definitions for the worker nodes.

using DataFrames
using HadoopBlocks

type WorkerCtx
    dfl::DataFrame
    dfr::DataFrame
    keycol::Symbol
    dfdict_left::Dict{Int, DataFrame}  # Parts of the left DataFrame to be
                                       # passed to other workers.
    dfdict_right::Dict{Int, DataFrame} # Parts of the right DataFrame to be
                                       # passed to other workers.
    accdf_left::DataFrame              # Accumulated left DataFrame
    accdf_right::DataFrame             # Accumulated right DataFrame
    joinresult::DataFrame

    WorkerCtx(dfl, dfr, keycol) = new(dfl, dfr, keycol, Dict(), Dict(),
                                      DataFrame(), DataFrame())
end

function Base.show(io::IO, ctx::WorkerCtx)
    procid = myid()
    print(io, "\n=================\n\nProcess: $procid\n\nDF array left: $(ctx.dfdict_left)\n\nDF array right: $(ctx.dfdict_right)\n\nAcc DF left: $(ctx.accdf_left)\n\nAcc DF right: $(ctx.accdf_right)\n===================\n")
end

# Determine which of the keys in `keyarr` belongs to which process from the
# hash of keys and process id's `keyhash`.
#
# Returns an dict of indexes of the dataframe. The array at index `i` of the
# return value represents the indexes for the process with worker id `i`.
function arrangement_idxs(keyarr, keyhash)
    idxs = Dict()
    for wid in g_wids
        idxs[wid] = Int[]
    end
    for i = 1:length(keyarr)
        procid = keyhash[keyarr[i]]
        push!(idxs[procid], i)
    end
    return idxs
end

# Get an dict of worker_id => sub_dataframes from the dataframe
# `df` and dict of indexes `idxs`.
function idxs_to_dfdict(df, idxs)
    np = length(idxs)
    dfdict = Dict()
    for wid in g_wids
        dfdict[wid] = df[idxs[wid], :]
    end
    return dfdict
end

# Given a tuple `blk` of filename and range and `headers`
# an array of header symbols, get a dataframe.
function get_df_from_block(blk, headers, sep)
    iobuff = Blocks.as_recordio(blk)
    # First chunk already has header
    if g_wids[1] == myid()
        df = readtable(iobuff, separator=sep)
    else
        df = readtable(iobuff, separator=sep, header=false, names=map(Symbol, headers))
    end
    close(iobuff)
    return df
end

# Intialize a global context of type `WorkerCtx`.  This function reads the chunks of
# files as given by the tuples `leftblk` and `righblk`.  `leftblk` and `rightblk` are
# tuples of (<filename>, <byte range to read>).  `lefheaders` and `rightheaders` are
# arrays of strings representing column names in the two tables.  `keycol` is the
# column symbol on which to join.  `wids` are the array of worker id's, the output of
# `workers()` on the manager machine.
function initworkerctx(leftblk, rightblk, leftheaders, rightheaders,
                       leftsep, rightsep, keycol, wids)
    global g_wids = wids
    dfl = get_df_from_block(leftblk, leftheaders, leftsep)
    dfr = get_df_from_block(rightblk, rightheaders, rightsep)
    global g_ctx = WorkerCtx(dfl, dfr, keycol)
end

# Count the occurances of each key. Returns a Dict of key => count.
function getkeycount()
    keys = [g_ctx.dfl[g_ctx.keycol]; g_ctx.dfr[g_ctx.keycol]]
    keycount = Dict()
    for key in keys
        if haskey(keycount, key)
            keycount[key] += 1
        else
            keycount[key] = 1
        end
    end
    return keycount
end

# Figure out what parts of the received dataframe should be given to what
# process. Store this in a global context. Also initialize the accumulator
# dataframe in the global context.
#
# Parameters:
# `keyhash`               : The keyhash generated on the name node (manager).
function initdistribution(keyhash)
    idxsl = arrangement_idxs(g_ctx.dfl[g_ctx.keycol], keyhash)
    idxsr = arrangement_idxs(g_ctx.dfr[g_ctx.keycol], keyhash)
    g_ctx.dfdict_left = idxs_to_dfdict(g_ctx.dfl, idxsl)
    g_ctx.dfdict_right = idxs_to_dfdict(g_ctx.dfr, idxsr)
    procid = myid()
    g_ctx.accdf_left = g_ctx.dfdict_left[procid]      # df belonging to self
    g_ctx.accdf_right = g_ctx.dfdict_right[procid]
end

# Send the sub-dataframe's to the respective processes. `typ` which
# is either `:left` or `:right`
# tells the receiving process wether this is to be accumulated in
# the left dataframe or right dataframe.
function pushrows(typ)
    procid = myid()
    dfdict = typ == :left ? g_ctx.dfdict_left : g_ctx.dfdict_right
    @sync for wid in g_wids
        size(dfdict[wid], 1) == 0 && continue # No need to send empty df's
        procid == wid && continue # No need to send to self
        @async remotecall_fetch(wid, recvrows, dfdict[wid], typ)
    end
end

# Recieve rows from other processes and concatenate them.
function recvrows(df, typ)
    if typ == :left
        g_ctx.accdf_left = vcat(g_ctx.accdf_left, df)
    else
        g_ctx.accdf_right = vcat(g_ctx.accdf_right, df)
    end
end

# Pass sub-dataframes to respective processes.
function communicate_push()
    pushrows(:left)
    pushrows(:right)
end

# Write the context to a file.  The file is number by the process id.  For
# example log of process id 2 will be log2.
function logdata()
    procid = myid()
    f = open("log$procid", "w")
    print(f, g_ctx)
    close(f)
end

# Join the left and right df belonging to you.
function joindf(kind)
    g_ctx.joinresult = join(g_ctx.accdf_left, g_ctx.accdf_right,
                           on=g_ctx.keycol, kind=kind)
    return g_ctx.joinresult
end

# Write the join result to a HDFS folder.
function writeresult(opfolder)
    pid = myid()
    idx = indexmap(g_wids)[pid]
    opfile = HDFSFile(joinpath(opfolder, "join" * idx))
    if idx == 1
        printtable(opfile, joinresult)
    else
        printtable(opfile, joinresult, header = false)
    end
end
