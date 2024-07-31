ChunkServer - holds chunks of a file
Client - user uploads or downloads files to chunk servers via client
Controller - interface between client and chunk servers  - contains information about chunk servers, but does not pass data
TCPChannel - describes TCP connections
TCPReceiverThread - describes a thread that receives messages
TCPSenderThread - describes a thread that sends messages
TCPServerThread - receives connections
Chunk - description of a piece of a file
ChunkServerRegisterRequest - registers a chunk server with the controller
ChunkServerRegisterResponse - response from controller to chunk server registration
ClientControllerChunkRetrievalResponse - controller to client contact on which chunk servers the client should contact
ClientControllerChunkRetrievalRequest - client to controller contact on which chunk servers the client should contact
ClientControllerChunkStorageRequest - client to controller contact on where to store chunks of a file
ClientControllerChunkStorageResponse - controller to client contact on where to store chunks of a file
ClientRegisterRequest - registering client with controller
ClientRegisterResponse - controller response to a registering client
ErrorCorrectionForward - controller to correct chunk server to forward a file to a bad chunk server
ErrorCorrectionRequest - chunk server to controller on how to correct a chunk
ErrorCorrectionResponse - chunk server to chunk server contact with a correct chunk
Event - events for messaging
EventFactory - handles event for event messaging
FailedNodeHelperRequest - controller to chunk server contact to replicate chunks that a failed chunk server had
MajorHeartbeat - chunk server to controller contact with information about all chunks a chunk server is storing
MinorHeartbeat - chunk server to controller contact with information about new chunks a chunk server is storing since last heartbeat
Protocol - enum for event types
RetrieveChunkRequest - client to chunk server request to receive a chunk of a file
RetrieveChunkResponse - chunk server to client response with a chunk of a file
StoreChunkRequest - client to controller request to find which chunk servers should store chunks of a file
StoreChunkUpdate -  client to chunk server request to store a chunk of a file
Node - interface for chunk servers, a client, and a controller