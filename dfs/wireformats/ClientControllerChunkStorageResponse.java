package csx55.dfs.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ClientControllerChunkStorageResponse implements Event {

    private int messageType;
    private List<String> chunkServers;
    private boolean success;

    public ClientControllerChunkStorageResponse(List<String> chunkServers, boolean success) {
        this.messageType = Protocol.CLIENT_CONTROLLER_CHUNK_STORAGE_RESPONSE.getValue();
        this.chunkServers = chunkServers;
        this.success = success;
    }

    public ClientControllerChunkStorageResponse(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);
        List<String> allChunkServers = new ArrayList<>();

        int messageType = dataInputStream.readInt();

        //always 3 for now
        for (int i=0; i<3; i++) {
            int chunkServerSize = dataInputStream.readInt();
            byte[] chunkServerBytes = new byte[chunkServerSize];
            dataInputStream.readFully(chunkServerBytes);
            String chunkServer = new String(chunkServerBytes, StandardCharsets.UTF_8);

            allChunkServers.add(chunkServer);
        }

        boolean success = dataInputStream.readBoolean();

        dataInputStream.close();
        baInputStream.close();

        this.chunkServers = allChunkServers;
        this.success = success;

    }

    public List<String> getChunkServers() {
        return this.chunkServers;
    }

    public boolean getSuccess() {
        return this.success;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.CLIENT_CONTROLLER_CHUNK_STORAGE_RESPONSE.getValue());

        for (String chunkServer : chunkServers) {
            byte[] chunkServerBytes = chunkServer.getBytes(StandardCharsets.UTF_8);
            dataOutputStream.writeInt(chunkServerBytes.length);
            dataOutputStream.write(chunkServerBytes);
        }

        dataOutputStream.writeBoolean(success);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
