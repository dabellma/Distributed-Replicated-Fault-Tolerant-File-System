package csx55.dfs.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class UpdateChunkServerRequest implements Event {

    private int messageType;
    private String chunkServer;

    public UpdateChunkServerRequest(String chunkServer) {
        this.messageType = Protocol.UPDATE_CHUNK_SERVER_REQUEST.getValue();
        this.chunkServer = chunkServer;
    }

    public UpdateChunkServerRequest(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        int chunkServerLength = dataInputStream.readInt();
        byte[] chunkServerBytes = new byte[chunkServerLength];
        dataInputStream.readFully(chunkServerBytes);
        String chunkServer = new String(chunkServerBytes, StandardCharsets.UTF_8);

        dataInputStream.close();
        baInputStream.close();

        this.chunkServer = chunkServer;
    }

    public String getChunkServer() {
        return this.chunkServer;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.UPDATE_CHUNK_SERVER_REQUEST.getValue());

        byte[] chunkServerBytes = chunkServer.getBytes(StandardCharsets.UTF_8);
        int chunkServerByteStringLength = chunkServerBytes.length;
        dataOutputStream.writeInt(chunkServerByteStringLength);
        dataOutputStream.write(chunkServerBytes);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
