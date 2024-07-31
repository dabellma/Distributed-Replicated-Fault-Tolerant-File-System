package csx55.dfs.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class StoreChunkRequest implements Event {

    private int messageType;
    private String fileName;
    private int chunkNumber;
    private int chunkLength;
    private byte[] chunk;
    private int numLeftSequence;
    private List<String> otherChunkServersForRedundancy;

    public StoreChunkRequest(String fileName, int chunkNumber, int chunkLength, byte[] chunk, int numLeftSequence, List<String> otherChunkServersForRedundancy) {
        this.messageType = Protocol.STORE_CHUNK_REQUEST.getValue();
        this.fileName = fileName;
        this.chunkNumber = chunkNumber;
        this.chunkLength = chunkLength;
        this.chunk = chunk;
        this.numLeftSequence = numLeftSequence;
        this.otherChunkServersForRedundancy = otherChunkServersForRedundancy;
    }

    public StoreChunkRequest(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);
        List<String> otherChunkServersForRedundancy = new ArrayList<>();

        int messageType = dataInputStream.readInt();

        int fileNameLength = dataInputStream.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        dataInputStream.readFully(fileNameBytes);
        String fileName = new String(fileNameBytes, StandardCharsets.UTF_8);

        int chunkNumber = dataInputStream.readInt();

        int chunkLength = dataInputStream.readInt();
        byte[] chunk = dataInputStream.readNBytes(chunkLength);

        int numLeftSequence = dataInputStream.readInt();

        for (int i=0; i<numLeftSequence; i++) {
            int chunkServerSize = dataInputStream.readInt();
            byte[] chunkServerBytes = new byte[chunkServerSize];
            dataInputStream.readFully(chunkServerBytes);
            String chunkServer = new String(chunkServerBytes, StandardCharsets.UTF_8);

            otherChunkServersForRedundancy.add(chunkServer);
        }


        dataInputStream.close();
        baInputStream.close();

        this.fileName = fileName;
        this.chunkNumber = chunkNumber;
        this.chunkLength = chunkLength;
        this.chunk = chunk;
        this.numLeftSequence = numLeftSequence;
        this.otherChunkServersForRedundancy = otherChunkServersForRedundancy;
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

    public List<String> getOtherChunkServersForRedundancy() {
        return this.otherChunkServersForRedundancy;
    }

    public int getNumLeftSequence() {
        return this.numLeftSequence;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.STORE_CHUNK_REQUEST.getValue());

        byte[] fileNameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        int byteStringLength = fileNameBytes.length;
        dataOutputStream.writeInt(byteStringLength);
        dataOutputStream.write(fileNameBytes);

        dataOutputStream.writeInt(chunkNumber);

        dataOutputStream.writeInt(chunkLength);
        dataOutputStream.write(chunk);

        dataOutputStream.writeInt(numLeftSequence);

        for (String chunkServer : otherChunkServersForRedundancy) {
            byte[] chunkServerBytes = chunkServer.getBytes(StandardCharsets.UTF_8);
            dataOutputStream.writeInt(chunkServerBytes.length);
            dataOutputStream.write(chunkServerBytes);
        }


        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
