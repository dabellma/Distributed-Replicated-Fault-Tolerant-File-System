package csx55.dfs.wireformats;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class EventFactory {
    private static final EventFactory instance = new EventFactory();

    private EventFactory() {}

    public static EventFactory getInstance() {
        return instance;
    }

    public Event createEvent(byte[] incomingMessage) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingMessage);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        if (messageType == Protocol.CHUNK_SERVER_REGISTER_REQUEST.getValue()) {
            ChunkServerRegisterRequest chunkServerRegisterRequest = new ChunkServerRegisterRequest(incomingMessage);
            return chunkServerRegisterRequest;
        } else if (messageType == Protocol.CHUNK_SERVER_REGISTER_RESPONSE.getValue()) {
            ChunkServerRegisterResponse chunkServerRegisterResponse = new ChunkServerRegisterResponse(incomingMessage);
            return chunkServerRegisterResponse;
        } else if (messageType == Protocol.ERROR_CORRECTION_REQUEST.getValue()) {
            ErrorCorrectionRequest errorCorrectionRequest = new ErrorCorrectionRequest(incomingMessage);
            return errorCorrectionRequest;
        } else if (messageType == Protocol.ERROR_CORRECTION_FORWARD.getValue()) {
            ErrorCorrectionForward errorCorrectionForward = new ErrorCorrectionForward(incomingMessage);
            return errorCorrectionForward;
        } else if (messageType == Protocol.ERROR_CORRECTION_RESPONSE.getValue()) {
            ErrorCorrectionResponse errorCorrectionResponse = new ErrorCorrectionResponse(incomingMessage);
            return errorCorrectionResponse;
        } else if (messageType == Protocol.CLIENT_REGISTER_REQUEST.getValue()) {
            ClientRegisterRequest clientRegisterRequest = new ClientRegisterRequest(incomingMessage);
            return clientRegisterRequest;
        } else if (messageType == Protocol.CLIENT_CONTROLLER_CHUNK_RETRIEVAL_REQUEST.getValue()) {
            ClientControllerChunkRetrievalRequest clientControllerChunkRetrievalRequest = new ClientControllerChunkRetrievalRequest(incomingMessage);
            return clientControllerChunkRetrievalRequest;
        } else if (messageType == Protocol.CLIENT_CONTROLLER_CHUNK_RETRIEVAL_RESPONSE.getValue()) {
            ClientControllerChunkRetrievalResponse clientControllerChunkRetrievalResponse = new ClientControllerChunkRetrievalResponse(incomingMessage);
            return clientControllerChunkRetrievalResponse;
        } else if (messageType == Protocol.CLIENT_CONTROLLER_CHUNK_STORAGE_REQUEST.getValue()) {
            ClientControllerChunkStorageRequest clientControllerChunkStorageRequest = new ClientControllerChunkStorageRequest(incomingMessage);
            return clientControllerChunkStorageRequest;
        } else if (messageType == Protocol.CLIENT_CONTROLLER_CHUNK_STORAGE_RESPONSE.getValue()) {
            ClientControllerChunkStorageResponse clientControllerChunkStorageResponse = new ClientControllerChunkStorageResponse(incomingMessage);
            return clientControllerChunkStorageResponse;
        } else if (messageType == Protocol.CLIENT_REGISTER_RESPONSE.getValue()) {
            ClientRegisterResponse clientRegisterResponse = new ClientRegisterResponse(incomingMessage);
            return clientRegisterResponse;
        } else if (messageType == Protocol.FAILED_NODE_HELPER_REQUEST.getValue()) {
            FailedNodeHelperRequest failedNodeHelperRequest = new FailedNodeHelperRequest(incomingMessage);
            return failedNodeHelperRequest;
        } else if (messageType == Protocol.MINOR_HEARTBEAT.getValue()) {
            MinorHeartbeat minorHeartbeat = new MinorHeartbeat(incomingMessage);
            return minorHeartbeat;
        } else if (messageType == Protocol.MAJOR_HEARTBEAT.getValue()) {
            MajorHeartbeat majorHeartbeat = new MajorHeartbeat(incomingMessage);
            return majorHeartbeat;
        } else if (messageType == Protocol.RETRIEVE_CHUNK_REQUEST.getValue()) {
            RetrieveChunkRequest retrieveChunkRequest = new RetrieveChunkRequest(incomingMessage);
            return retrieveChunkRequest;
        } else if (messageType == Protocol.RETRIEVE_CHUNK_RESPONSE.getValue()) {
            RetrieveChunkResponse retrieveChunkResponse = new RetrieveChunkResponse(incomingMessage);
            return retrieveChunkResponse;
        } else if (messageType == Protocol.STORE_CHUNK_REQUEST.getValue()) {
            StoreChunkRequest storeChunkRequest = new StoreChunkRequest(incomingMessage);
            return storeChunkRequest;
        } else if (messageType == Protocol.STORE_CHUNK_UPDATE.getValue()) {
            StoreChunkUpdate storeChunkUpdate = new StoreChunkUpdate(incomingMessage);
            return storeChunkUpdate;
        } else if (messageType == Protocol.UPDATE_CHUNK_SERVER_REQUEST.getValue()) {
            UpdateChunkServerRequest updateChunkServerRequest = new UpdateChunkServerRequest(incomingMessage);
            return updateChunkServerRequest;
        } else {
            System.out.println("Unrecognized event in event factory");

        }
        return null;
    }

}
