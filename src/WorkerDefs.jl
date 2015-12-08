# Definitions for the worker nodes.

using DataFrames
using Blocks

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

    WorkerCtx(dfl, dfr, keycol) = new(dfl, dfr, keycol, Dict(), Dict(),
                                      DataFrame(), DataFrame())
end

function Base.show(io::IO, ctx::WorkerCtx)
    procid = myid()
    print(io, "\n=================\n\nProcess: $procid\n\nDF array left: $(ctx.dfdict_left)\n\nDF array right: $(ctx.dfdict_right)\n\nAcc DF left: $(ctx.accdf_left)\n\nAcc DF right: $(ctx.accdf_right)\n===================\n")
end

# Determine which of the keys in `keyarr` belongs to which process from the
#  hash of keys and process id's `keyhash`.
# 
# Returns an dict of indexes of the dataframe. The array at index `i` of the
#  return value represents the indexes for the process with worker id `i`.
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
function get_df_from_block(blk, headers)
    iobuff = as_recordio(blk)
    # First chunk already has header
    df = g_wids[1] == myid() ? readtable(iobuff) : readtable(iobuff,
                                                             header=false,
                                                             names=map(Symbol, headers))
    close(iobuff)
    return df
end

# Intialize a global context of type `WorkerCtx`.  This function reads the chunks of
# files as given by the tuples `leftblk` and `righblk`.  `leftblk` and `rightblk` are
# tuples of (<filename>, <byte range to read>).  `lefheaders` and `rightheaders` are
# arrays of strings representing column names in the two tables.  `keycol` is the
# column symbol on which to join.  `wids` are the array of worker id's, the output of
# `workers()` on the manager machine. 
function initworkerctx(leftblk, rightblk, leftheaders, rightheaders, keycol, wids)
    global g_wids = wids
    dfl = get_df_from_block(leftblk, leftheaders)
    dfr = get_df_from_block(rightblk, rightheaders)
    global g_ctx = WorkerCtx(dfl, dfr, keycol)
end

# count the occurances of each key. Returns a Dict of key => count.
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

# Get the `typ` dataframe belonging to process with id `procid`.
getrows(procid, typ) = typ == :left ? g_ctx.dfdict_left[procid] : g_ctx.dfdict_right[procid]

# Pull rows of `typ` dataframe belonging to this worker process
# from the other worker processes. `typ` is :left or :right.
function pullrows(typ)
    procid = myid()
    refs = RemoteRef[]
    @sync for wid in g_wids
        wid == procid && continue
        @async begin
            ref = remotecall(wid, getrows, procid, typ)
            push!(refs, ref)
        end
    end
    for ref in refs
        df = fetch(ref)
        size(df, 1) == 0 && continue
        if typ == :left
            g_ctx.accdf_left = vcat(g_ctx.accdf_left, df)
        else
            g_ctx.accdf_right = vcat(g_ctx.accdf_right, df)
        end
    end
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

# Pass sub-dataframes to respective processes.
function communicate_pull()
    pullrows(:left)
    pullrows(:right)
end

function logdata()
    procid = myid()
    f = open("log$procid", "w")
    print(f, g_ctx)
    close(f)
end

# Join the left and right df belonging to you.
function joindf(keycol, kind)
    return join(g_ctx.accdf_left, g_ctx.accdf_right,
                on=keycol, kind=kind)
end

# Read a chunk of file `fn`.  The size of the chunk is to be calculated
# as `number_of_rows_in_fn / length(g_wids)`.  The starting point for reading
# is calculated as `(indexof(myid() in g_wids) - 1) * chunk_size + 1`.  If this
# is the last process then it should read to end of file.
# function readchunk(fn)
#     filesize = parse(split(readall(`wc -l $fn`))[1]) - 1 # -1 to exclude headers
#     chunksize = div(filesize, length(g_wids))
#     pid = myid()
#     idx = find(x -> x == pid, g_wids)[1]
#     startpt = (idx - 1) * chunksize + 1
#     endpt = idx == length(g_wids) ? filesize : startpt + chunksize - 1
#     return readchunk(fn, startpt, endpt)
# end
# 
# function readchunk(fn, startpt, endpt)
#     file = open(fn, "r")
#     chardata = readline(file) # read headers
# 
#     # seek till startpt
#     i = 1
#     while !eof(file) && i != startpt
#         readline(file)
#         i += 1
#     end
# 
#     # read till endpt
#     while !eof(file)
#         ln = readline(file)
#         chardata = chardata * ln
#         i == endpt && break
#         i += 1
#     end
# 
#     return readtable(IOBuffer(chardata))
# end
