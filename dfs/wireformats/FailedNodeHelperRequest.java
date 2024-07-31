package csx55.dfs.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class FailedNodeHelperRequest implements Event {

    private int messageType;
    private String fileName;
    private String chunkFilePath;
    private String chunkServerToStoreReplication;
    private int chunkNumber;

    public FailedNodeHelperRequest(String fileName, String chunkFilePath, String chunkServerToStoreReplication, int chunkNumber) {
        this.messageType = Protocol.FAILED_NODE_HELPER_REQUEST.getValue();
        this.fileName = fileName;
        this.chunkFilePath = chunkFilePath;
        this.chunkServerToStoreReplication = chunkServerToStoreReplication;
        this.chunkNumber = chunkNumber;
    }

    public FailedNodeHelperRequest(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        int fileNameLength = dataInputStream.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        dataInputStream.readFully(fileNameBytes);
        String fileName = new String(fileNameBytes, StandardCharsets.UTF_8);

        int chunkFilePathLength = dataInputStream.readInt();
        byte[] chunkFilePathBytes = new byte[chunkFilePathLength];
        dataInputStream.readFully(chunkFilePathBytes);
        String chunkFilePath = new String(chunkFilePathBytes, StandardCharsets.UTF_8);

        int chunkServerToStoreReplicationLength = dataInputStream.readInt();
        byte[] chunkServerToStoreReplicationBytes = new byte[chunkServerToStoreReplicationLength];
        dataInputStream.readFully(chunkServerToStoreReplicationBytes);
        String chunkServerToStoreReplication = new String(chunkServerToStoreReplicationBytes, StandardCharsets.UTF_8);

        int chunkNumber = dataInputStream.readInt();

        dataInputStream.close();
        baInputStream.close();

        this.fileName = fileName;
        this.chunkFilePath = chunkFilePath;
        this.chunkServerToStoreReplication = chunkServerToStoreReplication;
        this.chunkNumber = chunkNumber;
    }

    public String getFileName() {
        return this.fileName;
    }

    public String getChunkFilePath() {
        return this.chunkFilePath;
    }

    public String getChunkServerToStoreReplication() {
        return this.chunkServerToStoreReplication;
    }

    public int getChunkNumber() {
        return this.chunkNumber;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.FAILED_NODE_HELPER_REQUEST.getValue());

        byte[] fileNameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        int byteStringLength = fileNameBytes.length;
        dataOutputStream.writeInt(byteStringLength);
        dataOutputStream.write(fileNameBytes);

        byte[] chunkFilePathBytes = chunkFilePath.getBytes(StandardCharsets.UTF_8);
        int chunkFilePathByteStringLength = chunkFilePathBytes.length;
        dataOutputStream.writeInt(chunkFilePathByteStringLength);
        dataOutputStream.write(chunkFilePathBytes);

        byte[] chunkServerBytes = chunkServerToStoreReplication.getBytes(StandardCharsets.UTF_8);
        int chunkServerBytesLength = chunkServerBytes.length;
        dataOutputStream.writeInt(chunkServerBytesLength);
        dataOutputStream.write(chunkServerBytes);

        dataOutputStream.writeInt(chunkNumber);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
