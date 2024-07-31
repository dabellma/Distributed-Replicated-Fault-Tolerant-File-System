package csx55.dfs.wireformats;

import java.io.*;

public class ClientControllerChunkStorageRequest implements Event {

    private int messageType;

    public ClientControllerChunkStorageRequest() {
        this.messageType = Protocol.CLIENT_CONTROLLER_CHUNK_STORAGE_REQUEST.getValue();
    }

    public ClientControllerChunkStorageRequest(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        dataInputStream.close();
        baInputStream.close();

    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.CLIENT_CONTROLLER_CHUNK_STORAGE_REQUEST.getValue());

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
