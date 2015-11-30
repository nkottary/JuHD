# Definitions for the slave nodes.

using DataFrames

type ProcessCtx
    dfdict_left::Dict{Int, DataFrame}  # Parts of the left DataFrame to be
                                       # passed to other slaves.
    dfdict_right::Dict{Int, DataFrame} # Parts of the right DataFrame to be
                                       # passed to other slaves.
    accdf_left::DataFrame              # Accumulated left DataFrame
    accdf_right::DataFrame             # Accumulated right DataFrame
end

function Base.show(io::IO, ctx::ProcessCtx)
    procid = myid()
    print(io, "\n=================\n\nProcess: $procid\n\nDF array left: $(ctx.dfdict_left)\n\nDF array right: $(ctx.dfdict_right)\n\nAcc DF left: $(ctx.accdf_left)\n\nAcc DF right: $(ctx.accdf_right)\n===================\n")
end

# Determine which of the keys in `keyarr` belongs to which process from the
#  hash of keys and process id's `keyhash`.
# 
# Returns an dict of indexes of the dataframe. The array at index `i` of the
#  return value represents the indexes for the process with slave id `i`.
function arrangement_idxs(keyarr, keyhash)
    idxs = Dict()
    for sid in g_sids
        idxs[sid] = Int[]
    end
    for i = 1:length(keyarr)
        procid = keyhash[keyarr[i]]
        push!(idxs[procid], i)
    end
    return idxs
end

# Get an dict of slave_id => sub_dataframes from the dataframe
# `df` and dict of indexes `idxs`.
function idxs_to_dfdict(df, idxs)
    np = length(idxs)
    dfdict = Dict()
    for sid in g_sids
        dfdict[sid] = df[idxs[sid], :]
    end
    return dfdict
end

# Figure out what parts of the received dataframe should be given to what
# process. Store this in a global context. Also initialize the accumulator
# dataframe in the global context.
function initprocess(dfl, dfr, keyhash, keycol, sids)
    global g_sids = sids
    idxsl = arrangement_idxs(dfl[keycol], keyhash)
    idxsr = arrangement_idxs(dfr[keycol], keyhash)

    dfdict_left = idxs_to_dfdict(dfl, idxsl)
    dfdict_right = idxs_to_dfdict(dfr, idxsr)

    procid = myid()
    accdf_left = dfdict_left[procid]      # df belonging to self
    accdf_right = dfdict_right[procid]
    global g_ctx = ProcessCtx(dfdict_left, dfdict_right,
                              accdf_left, accdf_right)
end

# Get the `typ` dataframe belonging to process with id `procid`.
function getrows(procid, typ)
    if typ == :left
        return g_ctx.dfdict_left[procid]
    else
        return g_ctx.dfdict_right[procid]
    end
end

# Pull rows of `typ` dataframe belonging to this slave process
# from the other slave processes. `typ` is :left or :right.
function pullrows(typ)
    procid = myid()
    refs = RemoteRef[]
    @sync for sid in g_sids
        if sid == procid
            continue
        end
        @async begin
            ref = remotecall(sid, getrows, procid, typ)
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

# Send the sub-dataframe's to the respective processes. `typ` which
# is either `:left` or `:right`
# tells the receiving process wether this is to be accumulated in
# the left dataframe or right dataframe.
function pushrows(typ)
    procid = myid()
    if typ == :left
        dfdict = g_ctx.dfdict_left
    else
        dfdict = g_ctx.dfdict_right
    end
    @sync for sid in g_sids
        if (size(dfdict[sid], 1) == 0) # No need to send empty df's
            continue
        end
        if (procid == sid) # No need to send to self
            continue
        end
        @async remotecall(sid, recvrows, dfdict[sid], typ)
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
