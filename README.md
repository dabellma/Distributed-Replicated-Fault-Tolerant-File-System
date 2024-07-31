For this project I created a distributed, replicated, fault-tolerant file system that uses a client node, a controller node, and multiple chunk server nodes to store files in 64 MB chunks. This is similar to the Google File System from the 2003 research paper by researchers at Google (https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf). 

A user interacts with the client to store files in the system, and the client splits up the files into chunks to be stored on multiple chunk servers. The chunks of each file are replicated across 3 different chunk servers. These chunk servers communicate with the controller by providing heartbeats to the controller node that also contain information about all the chunks a particular chunk server contains. 


This file system also detects tampering with chunks when the client reads a file. If a user manually deletes or corrupts a file, the system detects this corruption and repairs it by using the other 2 replications of file chunk information to correct the corrupted file chunk. See the README in the "dfs" folder for further implementation details.
