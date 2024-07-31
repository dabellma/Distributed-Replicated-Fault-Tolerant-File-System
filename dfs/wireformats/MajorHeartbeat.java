package csx55.dfs.wireformats;

import csx55.dfs.util.Chunk;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MajorHeartbeat implements Event {

    private int messageType;
    private int totalNumberChunks;
    private int freeSpace;
    private int allChunksSize;
    private List<Chunk> allChunks;
    private String sendingNode;

    public MajorHeartbeat(int totalNumberChunks, int freeSpace, int allChunksSize, List<Chunk> allChunks, String sendingNode) {
        this.messageType = Protocol.MAJOR_HEARTBEAT.getValue();
        this.totalNumberChunks = totalNumberChunks;
        this.freeSpace = freeSpace;
        this.allChunksSize = allChunksSize;
        this.allChunks = allChunks;
        this.sendingNode = sendingNode;
    }

    public MajorHeartbeat(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);
        List<Chunk> allChunks = new ArrayList<>();

        int messageType = dataInputStream.readInt();

        int totalNumberChunks = dataInputStream.readInt();
        int freeSpace = dataInputStream.readInt();
        int allChunksSize = dataInputStream.readInt();

        for (int i = 0; i < allChunksSize; i++) {

            int fileNameSize = dataInputStream.readInt();
            byte[] fileNameBytes = new byte[fileNameSize];
            dataInputStream.readFully(fileNameBytes);
            String fileName = new String(fileNameBytes, StandardCharsets.UTF_8);

            int chunkFilePathSize = dataInputStream.readInt();
            byte[] chunkFilePathBytes = new byte[chunkFilePathSize];
            dataInputStream.readFully(chunkFilePathBytes);
            String chunkFilePath = new String(chunkFilePathBytes, StandardCharsets.UTF_8);

            int chunkServerSize = dataInputStream.readInt();
            byte[] chunkServerBytes = new byte[chunkServerSize];
            dataInputStream.readFully(chunkServerBytes);
            String chunkServer = new String(chunkServerBytes, StandardCharsets.UTF_8);

            int chunkNumber = dataInputStream.readInt();

            allChunks.add(new Chunk(fileName, chunkFilePath, chunkServer, chunkNumber));
        }

        int sendingNodeSize = dataInputStream.readInt();
        byte[] sendingNodeBytes = new byte[sendingNodeSize];
        dataInputStream.readFully(sendingNodeBytes);
        String sendingNode = new String(sendingNodeBytes, StandardCharsets.UTF_8);

        dataInputStream.close();
        baInputStream.close();

        this.totalNumberChunks = totalNumberChunks;
        this.freeSpace = freeSpace;
        this.allChunksSize = allChunksSize;
        this.allChunks = allChunks;
        this.sendingNode = sendingNode;
    }

    public int getAllChunksSize() {
        return allChunksSize;
    }
    public int getTotalNumberChunks() {
        return totalNumberChunks;
    }
    public List<Chunk> getAllChunks() {
        return allChunks;
    }
    public int getFreeSpace() {
        return freeSpace;
    }
    public String getSendingNode() {
        return sendingNode;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.MAJOR_HEARTBEAT.getValue());

        dataOutputStream.writeInt(totalNumberChunks);
        dataOutputStream.writeInt(freeSpace);
        dataOutputStream.writeInt(allChunksSize);

        for (Chunk chunk : allChunks) {
            byte[] fileNameBytes = chunk.getFileName().getBytes(StandardCharsets.UTF_8);
            dataOutputStream.writeInt(fileNameBytes.length);
            dataOutputStream.write(fileNameBytes);

            byte[] filePathBytes = chunk.getChunkFilePath().getBytes(StandardCharsets.UTF_8);
            dataOutputStream.writeInt(filePathBytes.length);
            dataOutputStream.write(filePathBytes);

            byte[] chunkServerBytes = chunk.getChunkServer().getBytes(StandardCharsets.UTF_8);
            dataOutputStream.writeInt(chunkServerBytes.length);
            dataOutputStream.write(chunkServerBytes);

            dataOutputStream.writeInt(chunk.getChunkNumber());
        }

        byte[] sendingNodeBytes = sendingNode.getBytes(StandardCharsets.UTF_8);
        dataOutputStream.writeInt(sendingNodeBytes.length);
        dataOutputStream.write(sendingNodeBytes);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
