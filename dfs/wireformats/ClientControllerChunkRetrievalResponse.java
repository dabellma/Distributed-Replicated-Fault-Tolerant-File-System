package csx55.dfs.wireformats;

import csx55.dfs.util.Chunk;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ClientControllerChunkRetrievalResponse implements Event {

    private int messageType;
    private int chunksToRecreateFileSize;
    private List<Chunk> chunksToRecreateFile;

    public ClientControllerChunkRetrievalResponse(int chunksToRecreateFileSize, List<Chunk> chunksToRecreateFile) {
        this.messageType = Protocol.CLIENT_CONTROLLER_CHUNK_RETRIEVAL_RESPONSE.getValue();
        this.chunksToRecreateFileSize = chunksToRecreateFileSize;
        this.chunksToRecreateFile = chunksToRecreateFile;
    }

    public ClientControllerChunkRetrievalResponse(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);
        List<Chunk> chunksToRecreateFile = new ArrayList<>();

        int messageType = dataInputStream.readInt();

        int chunksToRecreateFileSize = dataInputStream.readInt();

        for (int i = 0; i < chunksToRecreateFileSize; i++) {

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

            chunksToRecreateFile.add(new Chunk(fileName, chunkFilePath, chunkServer, chunkNumber));
        }

        dataInputStream.close();
        baInputStream.close();

        this.chunksToRecreateFileSize = chunksToRecreateFileSize;
        this.chunksToRecreateFile = chunksToRecreateFile;
    }

    public int getChunksToRecreateFileSize() {
        return chunksToRecreateFileSize;
    }

    public List<Chunk> getChunksToRecreateFile() {
        return chunksToRecreateFile;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.CLIENT_CONTROLLER_CHUNK_RETRIEVAL_RESPONSE.getValue());

        dataOutputStream.writeInt(chunksToRecreateFileSize);

        for (Chunk chunk : chunksToRecreateFile) {
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

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
