package csx55.dfs.transport;


import csx55.dfs.Node;
import csx55.dfs.replication.Controller;
import csx55.dfs.wireformats.Event;
import csx55.dfs.wireformats.EventFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public class TCPReceiverThread implements Runnable {

    private Socket socket;
    private DataInputStream dataInputStream;
    private Node node;
    private TCPChannel tcpChannel;

    public TCPReceiverThread(Socket socket, Node node, TCPChannel tcpChannel) throws IOException {
        this.socket = socket;
        this.node = node;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.tcpChannel = tcpChannel;

    }

    public DataInputStream getDataInputStream() {
        return this.dataInputStream;
    }

    @Override
    public void run() {
        while (socket != null) {
            try {

                //for this project, the protocol is to receive the length first
                int msgLength = dataInputStream.readInt();
                byte[] incomingMessage = new byte[msgLength];
                dataInputStream.readFully(incomingMessage, 0, msgLength);

                EventFactory eventFactory = EventFactory.getInstance();

                Event event = eventFactory.createEvent(incomingMessage);
                node.onEvent(event, tcpChannel);

            } catch (IOException | InterruptedException exception) {
                if (node instanceof Controller) {
                    Controller controllerInstance = (Controller) node;
                    String peerToRemove = null;
                    for (Map.Entry<String, TCPChannel> peer : controllerInstance.getTCPChannels().entrySet()) {
                        if (peer.getValue().equals(tcpChannel)) {
                            peerToRemove = peer.getKey();
                        }
                    }


                }
                break;
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
    }
}
