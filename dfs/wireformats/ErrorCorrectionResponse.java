package csx55.dfs.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ErrorCorrectionResponse implements Event {

    private int messageType;
    private String fileName;
    private int chunkNumber;
    private int chunkLength;
    private byte[] chunk;

    public ErrorCorrectionResponse(String fileName, int chunkNumber, int chunkLength, byte[] chunk) {
        this.messageType = Protocol.ERROR_CORRECTION_RESPONSE.getValue();
        this.fileName = fileName;
        this.chunkNumber = chunkNumber;
        this.chunkLength = chunkLength;
        this.chunk = chunk;
    }

    public ErrorCorrectionResponse(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        int fileNameLength = dataInputStream.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        dataInputStream.readFully(fileNameBytes);
        String fileName = new String(fileNameBytes, StandardCharsets.UTF_8);

        int chunkNumber = dataInputStream.readInt();

        int chunkLength = dataInputStream.readInt();
        byte[] chunk = dataInputStream.readNBytes(chunkLength);

        dataInputStream.close();
        baInputStream.close();

        this.fileName = fileName;
        this.chunkNumber = chunkNumber;
        this.chunkLength = chunkLength;
        this.chunk = chunk;
    }

    public String getFileName() {
        return this.fileName;
    }
    public int getChunkNumber() {
        return this.chunkNumber;
    }
    public int getChunkLength() {
        return this.chunkLength;
    }
    public byte[] getChunk() {
        return this.chunk;
    }


    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.ERROR_CORRECTION_RESPONSE.getValue());

        byte[] fileNameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        int byteStringLength = fileNameBytes.length;
        dataOutputStream.writeInt(byteStringLength);
        dataOutputStream.write(fileNameBytes);

        dataOutputStream.writeInt(chunkNumber);

        dataOutputStream.writeInt(chunkLength);
        dataOutputStream.write(chunk);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
