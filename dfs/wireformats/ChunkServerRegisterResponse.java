package csx55.dfs.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ChunkServerRegisterResponse implements Event {

    private int messageType;
    private int lengthAllChunkServers;
    private List<String> allChunkServers;

    public ChunkServerRegisterResponse(int lengthAllChunkServers, List<String> allChunkServers) {
        this.messageType = Protocol.CHUNK_SERVER_REGISTER_RESPONSE.getValue();
        this.lengthAllChunkServers = lengthAllChunkServers;
        this.allChunkServers = allChunkServers;
    }

    public ChunkServerRegisterResponse(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);
        List<String> allChunkServers = new ArrayList<>();

        int messageType = dataInputStream.readInt();
        int lengthAllChunkServers = dataInputStream.readInt();

        for (int i=0; i<lengthAllChunkServers; i++) {
            int chunkServerSize = dataInputStream.readInt();
            byte[] chunkServerBytes = new byte[chunkServerSize];
            dataInputStream.readFully(chunkServerBytes);
            String chunkServer = new String(chunkServerBytes, StandardCharsets.UTF_8);

            allChunkServers.add(chunkServer);
        }

        dataInputStream.close();
        baInputStream.close();

        this.lengthAllChunkServers = lengthAllChunkServers;
        this.allChunkServers = allChunkServers;

    }

    public int getLengthAllChunkServers() {
        return this.lengthAllChunkServers;
    }

    public List<String> getAllChunkServers() {
        return this.allChunkServers;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.CHUNK_SERVER_REGISTER_RESPONSE.getValue());
        dataOutputStream.writeInt(lengthAllChunkServers);

        for (String chunkServer : allChunkServers) {
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
