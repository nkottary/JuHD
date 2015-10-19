using Elly
using HadoopBlocks

dfs = HDFSClient("localhost", 9000)

node_files = HDFSFile(dfs, "/user/vagrant/node/node_1-100000001_aa")
## rel_files = HDFSFile(dfs, "/user/vagrant/relationship/relationship_1000000001-1100000001.gz")

node_blk = HadoopBlocks.Block(node_files)
num_node_blocks=length(node_blk.block)
println("number of node_blocks is :: $num_node_blocks")
chunk_data=Array{UInt8,1}()

for i=1:num_node_blocks
	start_offset = node_blk.block[i][2].start
	end_offset = node_blk.block[i][2].stop

	## println("The start and end offsets are :: $start_offset and $end_offset")
	chunk_data = [chunk_data, readbytes(open(node_blk.block[i][1]), end_offset-start_offset)]
	## println("The sizeof data chunk is ::: $(sizeof(chunk_data))")

end

file_length=length(chunk_data)
println("The total file size is ::: $(file_length)")
for m=1:file_length
   file_line=file_arr[m]
   line_arr=split(file_line, '\t')
   println("the pid is $(line_arr[2])")
end


#=
node_hdfs_blocks = hdfs_blocks(dfs, "/user/vagrant/node/node_1-100000001_aa.gz")
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

ugi = UserGroupInformation()
yarnclnt = YarnClient("localhost", 8032, ugi)

n = nodecount(yarnclnt)
nlist = nodes(yarnclnt)
yarnam = YarnAppMaster("localhost", 8030, ugi)

keys = nlist.status.keys
vals = nlist.status.vals

for i=1:length(keys)
	if(isdefined(keys, i))
    	println("the key is $(keys[i])")
    	println("the val is $(vals[i])")
	end
end
=#
