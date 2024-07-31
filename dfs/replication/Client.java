package csx55.dfs.replication;


import csx55.dfs.Node;
import csx55.dfs.transport.TCPChannel;
import csx55.dfs.transport.TCPServerThread;
import csx55.dfs.util.Chunk;
import csx55.dfs.wireformats.*;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

//todo tmp should be /tmp when turning in, if it's the same grading filesystem scheme
//as hw3
public class Client implements Node {

    public static final int CHUNK_SIZE = 64 * 1024;

    private TCPChannel controllerTcpChannel;

    private Map<String, TCPChannel> tcpChannels = new ConcurrentHashMap<>();
    private List<String> chunkServersToStoreMaxSizeThree = new ArrayList<>();
    private List<Chunk> chunkInformations;
    private Map<Integer, byte[]> downloadedChunks = new HashMap<>();

    private AtomicBoolean receivedChunkServersFromController = new AtomicBoolean(false);
    private AtomicBoolean receivedChunkRetrievalResponseFromController = new AtomicBoolean(false);
    private AtomicInteger chunksReceived = new AtomicInteger(0);

    private final String ipAddress;
    private final int portNumber;
    private String controllerIPAddress;
    private int controllerPortNumber;

    public Client(String ipAddress, int portNumber) {
        this.ipAddress = ipAddress;
        this.portNumber = portNumber;
    }

    public Client(String ipAddress, int portNumber, String controllerIPAddress, int controllerPortNumber) {
        this.ipAddress = ipAddress;
        this.portNumber = portNumber;
        this.controllerIPAddress = controllerIPAddress;
        this.controllerPortNumber = controllerPortNumber;
    }

    public String getIpAddress() {
        return this.ipAddress;
    }

    public int getPortNumber() {
        return this.portNumber;
    }

    public static void main(String[] args) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        if (args.length == 2) {

            try {
                ServerSocket serverSocket = new ServerSocket(0);

                System.out.println("Starting up client with ip address, and port number: " + " " + InetAddress.getLocalHost().getHostAddress() + ":" + serverSocket.getLocalPort());
                Client controller = new Client(InetAddress.getLocalHost().getHostAddress(), serverSocket.getLocalPort(), args[0], Integer.parseInt(args[1]));

                Thread peerServerThread = new Thread(new TCPServerThread(serverSocket, controller));
                peerServerThread.start();

                controller.registerWithController();

                while (true) {
                    String input;
                    input = reader.readLine();

                    controller.processInput(input);
                }

            } catch (IOException | InterruptedException e) {
                System.out.println("Encountered an issue while setting up chunk server: " + e);
                System.exit(1);
            }

        } else {
            System.out.println("Please enter exactly two arguments for the chunk server creation. Exiting.");
            System.exit(1);
        }

    }

    private void processInput(String input) throws IOException, InterruptedException {
        String[] tokens = input.split("\\s+");
        switch (tokens[0].toUpperCase()) {

            //todo add support for source and destination, not just destination
            case "DOWNLOAD":

                if (tokens.length != 2) {
                    System.out.println("Please provide a file to download... exactly 2 arguments: (download) (file).");
                    break;
                }

                String filetoDownload = Paths.get(tokens[1]).getFileName().toString();
                ClientControllerChunkRetrievalRequest clientControllerChunkRetrievalRequest = new ClientControllerChunkRetrievalRequest(filetoDownload);
                controllerTcpChannel.getTcpSenderThread().sendData(clientControllerChunkRetrievalRequest.getbytes());

                while(!receivedChunkRetrievalResponseFromController.get()) {
                    Thread.sleep(50);
                }
                receivedChunkRetrievalResponseFromController.set(false);

                for (int i=0; i<chunkInformations.size(); i++) {
                    Chunk chunk = chunkInformations.get(i);
                    ChunkServer chunkServerHoldingChunk = chunkServerDestring(chunk.getChunkServer());
                    TCPChannel chunkServerChannel = getOrCreateConnection(chunkServerHoldingChunk.getIpAddress(), chunkServerHoldingChunk.getPortNumber());
                    RetrieveChunkRequest retrieveChunkRequest = new RetrieveChunkRequest(filetoDownload, i);
                    chunkServerChannel.getTcpSenderThread().sendData(retrieveChunkRequest.getbytes());
                }

                System.out.println("Waiting to receive chunks....");
                while(!(chunksReceived.get() == chunkInformations.size())) {
                    Thread.sleep(50);
                }

                //todo change the output path
                File outputFile = new File("reconstructed_number.txt");

                System.out.println("Writing to output file...");
                try {
                    FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
                    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);

                    List<Map.Entry<Integer, byte[]>> allEntries = new ArrayList<>(downloadedChunks.entrySet());

                    for (int i = 0; i < allEntries.size(); i++) {
                        Map.Entry<Integer, byte[]> entry = allEntries.get(i);
                        bufferedOutputStream.write(entry.getValue());
                    }

                    bufferedOutputStream.flush();

                } catch (IOException e) {
                    e.printStackTrace();
                }

                //reset asynchronous communication parameters and downloaded chunks
                chunkInformations = new ArrayList<>();
                chunksReceived.set(0);
                downloadedChunks = new HashMap<>();
                break;

            case "LIST-CONNECTIONS":
                for (String connection : tcpChannels.keySet()) {
                    System.out.println(connection);
                }
                break;

                //todo add support for source and destination, not just source
            case "UPLOAD":
                if (tokens.length != 2) {
                    System.out.println("Please provide a file when uploading... exactly 2 arguments: (upload) (file).");
                    break;
                }

                List<byte[]> fileChunks = new ArrayList<>();
                byte[] byteBuffer = new byte[CHUNK_SIZE];
                int numberOfReadBytes;

                File incomingFile = new File(tokens[1]);
                String fileName = incomingFile.getName();
                FileInputStream fileInputStream = new FileInputStream(incomingFile);
                BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                while ((numberOfReadBytes = bufferedInputStream.read(byteBuffer)) > 0) {
                    byte[] chunkBytes = new byte[numberOfReadBytes];
                    System.arraycopy(byteBuffer, 0, chunkBytes, 0, numberOfReadBytes);
                    fileChunks.add(chunkBytes);

                }

                //for each chunk, store it on 3 other nodes
                for (int i=0; i<fileChunks.size(); i++) {
                    byte[] chunk = fileChunks.get(i);
                    System.out.println(chunk);

                    ClientControllerChunkStorageRequest clientControllerChunkStorageRequest = new ClientControllerChunkStorageRequest();
                    controllerTcpChannel.getTcpSenderThread().sendData(clientControllerChunkStorageRequest.getbytes());

                    //wait 5 seconds to get a response from the controller before timing out
                    int cyclesWaited = 0;
                    while(!this.receivedChunkServersFromController.get()) {
                        if (cyclesWaited > 100) {
                            System.out.println("Error receiving chunk servers from controller");
                            break;
                        }
                        Thread.sleep(50);
                        cyclesWaited++;
                    }

                    receivedChunkServersFromController.set(false);

                    String firstChunkServer = this.chunkServersToStoreMaxSizeThree.get(0);
                    this.chunkServersToStoreMaxSizeThree.remove(0);
                    StoreChunkRequest storeChunkRequest = new StoreChunkRequest(fileName, i, chunk.length, chunk, 2, this.chunkServersToStoreMaxSizeThree);
                    ChunkServer firstChunkServerToSendChunk = chunkServerDestring(firstChunkServer);

                    System.out.println("Sending chunk bytes: " + storeChunkRequest.getChunk());
                    TCPChannel chunkServerConnection = getOrCreateConnection(firstChunkServerToSendChunk.getIpAddress(), firstChunkServerToSendChunk.getPortNumber());
                    chunkServerConnection.getTcpSenderThread().sendData(storeChunkRequest.getbytes());


                    this.chunkServersToStoreMaxSizeThree = new ArrayList<>();

                }

                break;

            default:
                System.out.println("Unknown command. Re-entering wait period.");
                break;
        }

    }

    @Override
    public void onEvent(Event event, TCPChannel tcpChannel) throws IOException, InterruptedException {
        if (event instanceof ClientRegisterResponse) {

            ClientRegisterResponse clientRegisterResponse = (ClientRegisterResponse) event;
            System.out.println("got client register response");


        } else if (event instanceof ClientControllerChunkRetrievalResponse) {

            ClientControllerChunkRetrievalResponse clientControllerChunkRetrievalResponse = (ClientControllerChunkRetrievalResponse) event;
            chunkInformations = clientControllerChunkRetrievalResponse.getChunksToRecreateFile();
            receivedChunkRetrievalResponseFromController.set(true);

        } else if (event instanceof ClientControllerChunkStorageResponse) {

            ClientControllerChunkStorageResponse clientControllerChunkStorageResponse = (ClientControllerChunkStorageResponse) event;
            if (clientControllerChunkStorageResponse.getSuccess()) {
                System.out.println("Got successful ClientControllerChunkStorageResponse!");

                addChunkServersMaxSizeThree(clientControllerChunkStorageResponse.getChunkServers());
                receivedChunkServersFromController.set(true);
            } else {
                System.out.println("Not enough chunk servers for 3-fold replication, did not save chunk");
            }


        } else if (event instanceof ClientRegisterRequest) {
            ClientRegisterRequest clientRegisterRequest = (ClientRegisterRequest) event;
            String newPeer = new Client(clientRegisterRequest.getIpAddress(), clientRegisterRequest.getPortNumber()).toString();


        } else if (event instanceof RetrieveChunkResponse) {
            System.out.println("Got RetrieveChunkResponse!");

            RetrieveChunkResponse retrieveChunkResponse = (RetrieveChunkResponse) event;
            addFileChunk(retrieveChunkResponse.getChunkNumber(), retrieveChunkResponse.getChunk());
            chunksReceived.getAndIncrement();

        } else {
            System.out.println("Received unknown event.");
        }
    }

    private TCPChannel getOrCreateConnection(String ipAddress, int port) throws IOException {
        String node = ipAddress + ":" + port;
        TCPChannel existingTcpChannel = tcpChannels.get(node);
        if (existingTcpChannel == null) {
            Socket socketToOtherPeerToConnectWith = new Socket(ipAddress, port);
            TCPChannel tcpChannelOtherPeer = new TCPChannel(socketToOtherPeerToConnectWith, this);

            Thread receivingThread = new Thread(tcpChannelOtherPeer.getTcpReceiverThread());
            receivingThread.start();

            Thread sendingThread = new Thread(tcpChannelOtherPeer.getTcpSenderThread());
            sendingThread.start();

            tcpChannels.put(ipAddress + ":" + port, tcpChannelOtherPeer);

            existingTcpChannel = tcpChannelOtherPeer;
        }
        return existingTcpChannel;
    }

    private synchronized void addChunkServersMaxSizeThree(List<String> chunkServers) {
        this.chunkServersToStoreMaxSizeThree.addAll(chunkServers);
    }

    private synchronized void addFileChunk(int chunkNumber, byte[] chunk) {
        this.downloadedChunks.put(chunkNumber, chunk);
    }


    private void registerWithController() throws IOException, InterruptedException {

        Socket socketToHost = new Socket(this.controllerIPAddress, this.controllerPortNumber);
        TCPChannel tcpChannel = new TCPChannel(socketToHost, this);

        Thread receivingThread = new Thread(tcpChannel.getTcpReceiverThread());
        receivingThread.start();

        Thread sendingThread = new Thread(tcpChannel.getTcpSenderThread());
        sendingThread.start();

        controllerTcpChannel = tcpChannel;

        ClientRegisterRequest clientRegisterRequest = new ClientRegisterRequest(this.getIpAddress(), this.getPortNumber());
        controllerTcpChannel.getTcpSenderThread().sendData(clientRegisterRequest.getbytes());
    }

    private ChunkServer chunkServerDestring(String chunkServerAsString) {
        String[] ipAddressAndPort = chunkServerAsString.split(":");
        String ipAddress = ipAddressAndPort[0];
        int portNumber = Integer.parseInt(ipAddressAndPort[1]);
        return new ChunkServer(ipAddress, portNumber);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        Client controllerObject = (Client) object;
        return portNumber == controllerObject.portNumber && Objects.equals(ipAddress, controllerObject.ipAddress);
    }

    @Override
    public String toString() {
        return ipAddress + ":" + portNumber;
    }
}
