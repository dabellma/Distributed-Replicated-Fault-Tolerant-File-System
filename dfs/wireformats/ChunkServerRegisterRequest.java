package csx55.dfs.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ChunkServerRegisterRequest implements Event {

    private int messageType;
    private String ipAddress;
    private int portNumber;

    public ChunkServerRegisterRequest(String ipAddress, int portNumber) {
        this.messageType = Protocol.CHUNK_SERVER_REGISTER_REQUEST.getValue();
        this.ipAddress = ipAddress;
        this.portNumber = portNumber;
    }

    public ChunkServerRegisterRequest(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        int ipAddressLength = dataInputStream.readInt();
        byte[] ipAddressBytes = new byte[ipAddressLength];
        dataInputStream.readFully(ipAddressBytes);
        String ipAddress = new String(ipAddressBytes, StandardCharsets.UTF_8);

        int portNumber = dataInputStream.readInt();

        dataInputStream.close();
        baInputStream.close();

        this.ipAddress = ipAddress;
        this.portNumber = portNumber;
    }

    public String getIpAddress() {
        return this.ipAddress;
    }
    public int getPortNumber() {
        return this.portNumber;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.CHUNK_SERVER_REGISTER_REQUEST.getValue());

        byte[] ipAddressBytes = ipAddress.getBytes(StandardCharsets.UTF_8);
        int byteStringLength = ipAddressBytes.length;
        dataOutputStream.writeInt(byteStringLength);
        dataOutputStream.write(ipAddressBytes);

        dataOutputStream.writeInt(portNumber);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
