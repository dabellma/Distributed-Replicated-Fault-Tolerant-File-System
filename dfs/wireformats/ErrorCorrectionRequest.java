package csx55.dfs.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ErrorCorrectionRequest implements Event {

    private int messageType;
    private String fileName;
    private int chunkNumber;
    private String brokenChunkServer;

    public ErrorCorrectionRequest(String fileName, int chunkNumber, String brokenChunkServer) {
        this.messageType = Protocol.ERROR_CORRECTION_REQUEST.getValue();
        this.fileName = fileName;
        this.chunkNumber = chunkNumber;
        this.brokenChunkServer = brokenChunkServer;
    }

    public ErrorCorrectionRequest(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        int fileNameLength = dataInputStream.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        dataInputStream.readFully(fileNameBytes);
        String fileName = new String(fileNameBytes, StandardCharsets.UTF_8);

        int chunkNumber = dataInputStream.readInt();

        int brokenChunkServerLength = dataInputStream.readInt();
        byte[] brokenChunkServerBytes = new byte[brokenChunkServerLength];
        dataInputStream.readFully(brokenChunkServerBytes);
        String brokenChunkServer = new String(brokenChunkServerBytes, StandardCharsets.UTF_8);

        dataInputStream.close();
        baInputStream.close();

        this.fileName = fileName;
        this.chunkNumber = chunkNumber;
        this.brokenChunkServer = brokenChunkServer;
    }

    public String getFileName() {
        return this.fileName;
    }
    public int getChunkNumber() {
        return this.chunkNumber;
    }

    public String getBrokenChunkServer() {
        return brokenChunkServer;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.ERROR_CORRECTION_REQUEST.getValue());

        byte[] fileNameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        int byteStringLength = fileNameBytes.length;
        dataOutputStream.writeInt(byteStringLength);
        dataOutputStream.write(fileNameBytes);

        dataOutputStream.writeInt(chunkNumber);

        byte[] brokenChunkServerBytes = brokenChunkServer.getBytes(StandardCharsets.UTF_8);
        int brokenChunkServerByteStringLength = brokenChunkServerBytes.length;
        dataOutputStream.writeInt(brokenChunkServerByteStringLength);
        dataOutputStream.write(brokenChunkServerBytes);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
