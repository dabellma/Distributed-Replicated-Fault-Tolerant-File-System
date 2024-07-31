package csx55.dfs.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class StoreChunkUpdate implements Event {

    private int messageType;
    private String fileName;
    private String chunkFilePath;
    private String chunkServer;
    private int chunkNumber;

    public StoreChunkUpdate(String fileName, String chunkFilePath, String chunkServer, int chunkNumber) {
        this.messageType = Protocol.STORE_CHUNK_UPDATE.getValue();
        this.fileName = fileName;
        this.chunkFilePath = chunkFilePath;
        this.chunkServer = chunkServer;
        this.chunkNumber = chunkNumber;
    }

    public StoreChunkUpdate(byte[] incomingByteArray) throws IOException {
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

        int chunkServerLength = dataInputStream.readInt();
        byte[] chunkServerBytes = new byte[chunkServerLength];
        dataInputStream.readFully(chunkServerBytes);
        String chunkServer = new String(chunkServerBytes, StandardCharsets.UTF_8);

        int chunkNumber = dataInputStream.readInt();

        dataInputStream.close();
        baInputStream.close();

        this.fileName = fileName;
        this.chunkFilePath = chunkFilePath;
        this.chunkServer = chunkServer;
        this.chunkNumber = chunkNumber;
    }

    public String getFileName() {
        return this.fileName;
    }

    public String getChunkFilePath() {
        return this.chunkFilePath;
    }

    public String getChunkServer() {
        return this.chunkServer;
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

        dataOutputStream.writeInt(Protocol.STORE_CHUNK_UPDATE.getValue());

        byte[] fileNameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        int byteStringLength = fileNameBytes.length;
        dataOutputStream.writeInt(byteStringLength);
        dataOutputStream.write(fileNameBytes);

        byte[] chunkFilePathBytes = chunkFilePath.getBytes(StandardCharsets.UTF_8);
        int chunkFilePathByteStringLength = chunkFilePathBytes.length;
        dataOutputStream.writeInt(chunkFilePathByteStringLength);
        dataOutputStream.write(chunkFilePathBytes);

        byte[] chunkServerBytes = chunkServer.getBytes(StandardCharsets.UTF_8);
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
