using Elly
using HadoopBlocks

## localhost
#=
host="localhost"; port=9000;
yarn_host="localhost"; yarn_port=8032
## yarnam = YarnAppMaster("localhost", 8030, ugi)

node_file_name="/user/vagrant/node/node_1-100000001_aa"
## rel_file_name="/user/vagrant/relationship/relationship_1000000001-1100000001.gz"
=#

## server
## #=
host="ip-10-11-191-51.ec2.internal"; port=8020;
yarn_host="ip-10-35-204-38.ec2.internal"; yarn_port=8050

## node_file_name="/user/mapred/julia/node/node_1-100000001_aa.gz"
node_file_name="/user/mapred/dev/node/node"
## rel_file_name="/user/vagrant/relationship/relationship_1000000001-1100000001.gz"

## =#


dfs = HDFSClient(host, port)

ugi = UserGroupInformation()
yarnclnt = YarnClient(yarn_host, yarn_port, ugi)

n = nodecount(yarnclnt)
nlist = nodes(yarnclnt)

keys = nlist.status.keys
vals = nlist.status.vals

machines=Array{ASCIIString,}(nlist.status.count)
real_count=0
for i=1:length(keys)
	if(Int(nlist.status.slots[i]) == 1)
		## push!(machines, nlist.status.keys[i].host)	
		println("The host is :: $(nlist.status.keys[i].host)")
		real_count += 1
		machines[real_count] = nlist.status.keys[i].host
	end
end
## @show machines

addprocs(machines)
## addprocs(machines; tunnel=true, sshflags="-i /home/centos/.ssh/id_rsa")

## This should be the actual function !!!
@everywhere myfunc=function myfunc()
	return myid()*10
end


remote_refs=Array{RemoteRef{Channel{Any}},}(length(machines))

## remote_refs=Array{ASCIIString,}(length(machines))
@sync for j=1:length(machines)
	## make remote call asynchronously
    @async remote_refs[j]=remotecall(myfunc, workers()[j])	

end

# Fetch results (status)
for k=1:length(machines)
    ## result = remotecall_fetch(myfunc, remote_refs[k].where)
    result = fetch(remote_refs[k])
	println("The results on node $(remote_refs[k].where) is $result")
end

node_fr = Elly.HDFSFileReader(dfs, node_file_name)

node_df=DataFrame{Int64, AbstractString, AbstractString}()
node_line=readline(node_fr)
while(!isempty(node_line))
	println(node_line)
	push!(node_df, 
	node_line = readline(node_fr)
end

rel_fr = Elly.HDFSFileReader(dfs, rel_file_name)


#=
actual_count=0
for i=1:length(keys)
	if(isdefined(keys, i))
    	## println("the key is $(keys[i])")
    	## println("the val is $(vals[i])")
		actual_count+=1
	end
end
=#k

#=
node_hdfs_blocks = hdfs_blocks(dfs, "/user/vagrant/node/node_1-100000001_aa.gz")
node_hdfs_blocks = hdfs_blocks(dfs, node_file_name)
## rel_hdfs_blocks = hdfs_blocks(dfs, "/user/vagrant/relationship/relationship_1000000001-1100000001.gz")

hdfs_blocks=node_hdfs_blocks
num_blocks = length(hdfs_blocks)
for i=1:num_blocks
	node_offset = Int(hdfs_blocks[i][1])
	println("The node offset is :: $node_offset") 

	num_chunks = length(hdfs_blocks[1][2])
	for j=1:num_chunks
		node_ip = hdfs_blocks[i][2][j]
		println("The node IP are :: $node_ip") 

		## read chunk

		## sort chunk
	end
end
=#

#=
fr = Elly.HDFSFileReader(dfs, "/user/vagrant/Batting.csv")
batting_file = HDFSFile(dfs, "/user/vagrant/Batting.csv")
=#

node_file = HDFSFile(dfs, node_file_name)
## rel_file = HDFSFile(dfs, rel_file_name)

node_blk = HadoopBlocks.Block(node_file)
num_node_blocks=length(node_blk.block)
println("number of node_blocks is :: $num_node_blocks")
chunk_data=Array{UInt8,1}()

df = DataFrame(
#=
for i=1:num_node_blocks
	start_offset = node_blk.block[i][2].start
	end_offset = node_blk.block[i][2].stop

	## println("The start and end offsets are :: $start_offset and $end_offset")
	chunk_data = [chunk_data, readbytes(open(node_blk.block[i][1]), end_offset-start_offset)]
	## println("The sizeof data chunk is ::: $(sizeof(chunk_data))")
end
=#

file_length=length(chunk_data)
println("The total file size is ::: $(file_length)")
for m=1:file_length
   file_line=chunk_data[m]
   line_arr=split(file_line, '\t')
   println("the pid is $(line_arr[2])")
end

