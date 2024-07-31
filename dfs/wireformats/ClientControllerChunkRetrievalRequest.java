package csx55.dfs.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ClientControllerChunkRetrievalRequest implements Event {

    private int messageType;
    private String file;

    public ClientControllerChunkRetrievalRequest(String file) {
        this.messageType = Protocol.CLIENT_CONTROLLER_CHUNK_RETRIEVAL_REQUEST.getValue();
        this.file = file;
    }

    public ClientControllerChunkRetrievalRequest(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        int ipAddressLength = dataInputStream.readInt();
        byte[] ipAddressBytes = new byte[ipAddressLength];
        dataInputStream.readFully(ipAddressBytes);
        String ipAddress = new String(ipAddressBytes, StandardCharsets.UTF_8);

        dataInputStream.close();
        baInputStream.close();

        this.file = ipAddress;
    }

    public String getFile() {
        return this.file;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.CLIENT_CONTROLLER_CHUNK_RETRIEVAL_REQUEST.getValue());

        byte[] ipAddressBytes = file.getBytes(StandardCharsets.UTF_8);
        int byteStringLength = ipAddressBytes.length;
        dataOutputStream.writeInt(byteStringLength);
        dataOutputStream.write(ipAddressBytes);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
