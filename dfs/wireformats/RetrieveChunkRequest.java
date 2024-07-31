package csx55.dfs.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class RetrieveChunkRequest implements Event {

    private int messageType;
    private String fileName;
    private int chunkNumber;

    public RetrieveChunkRequest(String fileName, int chunkNumber) {
        this.messageType = Protocol.RETRIEVE_CHUNK_REQUEST.getValue();
        this.fileName = fileName;
        this.chunkNumber = chunkNumber;
    }

    public RetrieveChunkRequest(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        int fileNameLength = dataInputStream.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        dataInputStream.readFully(fileNameBytes);
        String fileName = new String(fileNameBytes, StandardCharsets.UTF_8);

        int chunkNumber = dataInputStream.readInt();

        dataInputStream.close();
        baInputStream.close();

        this.fileName = fileName;
        this.chunkNumber = chunkNumber;
    }

    public String getFileName() {
        return this.fileName;
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

        dataOutputStream.writeInt(Protocol.RETRIEVE_CHUNK_REQUEST.getValue());

        byte[] fileNameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        int byteStringLength = fileNameBytes.length;
        dataOutputStream.writeInt(byteStringLength);
        dataOutputStream.write(fileNameBytes);

        dataOutputStream.writeInt(chunkNumber);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
