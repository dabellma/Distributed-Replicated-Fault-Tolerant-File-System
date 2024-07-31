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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChunkServer implements Node {

    private TCPChannel controllerTcpChannel;

    private Map<String, TCPChannel> tcpChannels = new ConcurrentHashMap<>();
    //todo probably can deprecate/not use this anymore
    //i don't think the other chunk servers are needed
    private List<String> otherChunkServers = new ArrayList<>();
    private List<Chunk> chunks = new ArrayList<>();
    private List<Chunk> newlyAddedChunks = new ArrayList<>();
    private AtomicBoolean fixedTampering = new AtomicBoolean(false);

    private final String ipAddress;
    private final int portNumber;
    private String controllerIPAddress;
    private int controllerPortNumber;

    public ChunkServer(String ipAddress, int portNumber) {
        this.ipAddress = ipAddress;
        this.portNumber = portNumber;
    }

    public ChunkServer(String ipAddress, int portNumber, String controllerIPAddress, int controllerPortNumber) {
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

                System.out.println("Starting up chunk server with ip address, and port number: " + " " + InetAddress.getLocalHost().getHostAddress() + ":" + serverSocket.getLocalPort());
                ChunkServer chunkServer = new ChunkServer(InetAddress.getLocalHost().getHostAddress(), serverSocket.getLocalPort(), args[0], Integer.parseInt(args[1]));

                Thread peerServerThread = new Thread(new TCPServerThread(serverSocket, chunkServer));
                peerServerThread.start();

                chunkServer.registerWithController();

                int minorHeartbeats = 0;
                while (true) {
                    Thread.sleep(15000);
                    minorHeartbeats++;

                    if (minorHeartbeats == 8) {
                        //send major heartbeat
                        MajorHeartbeat majorHeartbeat = new MajorHeartbeat(chunkServer.chunks.size(), 0, chunkServer.chunks.size(), chunkServer.chunks, chunkServer.toString());
                        TCPChannel controllerConnection = chunkServer.getOrCreateConnection(chunkServer.controllerIPAddress, chunkServer.controllerPortNumber);
                        controllerConnection.getTcpSenderThread().sendData(majorHeartbeat.getbytes());

                        minorHeartbeats = 0;
                    } else {
                        //send minor heartbeat
                        MinorHeartbeat minorHeartbeat = new MinorHeartbeat(chunkServer.chunks.size(), 0, chunkServer.newlyAddedChunks.size(), chunkServer.newlyAddedChunks, chunkServer.toString());
                        TCPChannel controllerConnection = chunkServer.getOrCreateConnection(chunkServer.controllerIPAddress, chunkServer.controllerPortNumber);
                        controllerConnection.getTcpSenderThread().sendData(minorHeartbeat.getbytes());

                        //set the chunk server's newly added chunks back to an arraylist; it might've already been empty
                        chunkServer.newlyAddedChunks = new ArrayList<>();
                    }


//                    String input;
//                    input = reader.readLine();
//
//                    controller.processInput(input);
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

            case "CONNECTIONS":
                for (String connection : tcpChannels.keySet()) {
                    System.out.println(connection);
                }
                break;
            case "OTHER-CHUNK-SERVERS":
                for (String otherChunkServer : otherChunkServers) {
                    System.out.println(otherChunkServer);
                }
                break;
            case "CHUNKS":
                for (Chunk chunk : this.chunks) {
                    System.out.println("Chunk file name: " + chunk.getFileName() + " chunk file path: " + chunk.getChunkFilePath() +
                            " chunk number: " + chunk.getChunkNumber());
//                    String.format("Chunk file name: %s, chunk file path: %s, chunk number: %d, chunk bytes:");
                }
                break;
            default:
                System.out.println("Unknown command. Re-entering wait period.");
                break;
        }

    }

    @Override
    public void onEvent(Event event, TCPChannel tcpChannel) throws IOException, InterruptedException, NoSuchAlgorithmException {

        if (event instanceof ChunkServerRegisterResponse) {

            ChunkServerRegisterResponse chunkServerRegisterResponse = (ChunkServerRegisterResponse) event;
            addOtherChunkServers(chunkServerRegisterResponse.getAllChunkServers());

            System.out.println("got chunk server register response");


        } else if (event instanceof ErrorCorrectionForward) {

            ErrorCorrectionForward errorCorrectionForward = (ErrorCorrectionForward) event;
            ChunkServer brokenChunkServer = chunkServerDestring(errorCorrectionForward.getBrokenChunkServer());

            Chunk correctChunk = null;
            for (Chunk chunk : this.chunks) {
                if (chunk.getFileName().equals(errorCorrectionForward.getFileName())
                        && (chunk.getChunkNumber() == errorCorrectionForward.getChunkNumber())) {
                    correctChunk = chunk;
                    break;
                }
            }

            ErrorCorrectionResponse errorCorrectionResponse = new ErrorCorrectionResponse(correctChunk.getFileName(), correctChunk.getChunkNumber(), correctChunk.getChunk().length, correctChunk.getChunk());
            TCPChannel brokenChunkServerConnection = getOrCreateConnection(brokenChunkServer.getIpAddress(), brokenChunkServer.getPortNumber());
            brokenChunkServerConnection.getTcpSenderThread().sendData(errorCorrectionResponse.getbytes());

        } else if (event instanceof ErrorCorrectionResponse) {

            ErrorCorrectionResponse errorCorrectionResponse = (ErrorCorrectionResponse) event;

            /*
            all the same code as store chunk request, except for the ending notifications
             */

            //todo remove the "this.get ip address" and the last slash
            String directoryPath = "tmp/chunk-server/project/" + this.getIpAddress() + "/";
            String chunkFilePath = String.format("%s_chunk%d", errorCorrectionResponse.getFileName(), errorCorrectionResponse.getChunkNumber());
            String fullPath = directoryPath + chunkFilePath;

            Map<Integer, byte[]> chunkSliceChecksums = computeChunkChecksums(errorCorrectionResponse.getChunk());

            Chunk chunk = new Chunk(errorCorrectionResponse.getFileName(), fullPath, this.toString(), errorCorrectionResponse.getChunkNumber(), errorCorrectionResponse.getChunk(), chunkSliceChecksums);
            addChunk(chunk);

            //and actually save the chunk
            File directory = new File(directoryPath);
            if (!directory.exists()) {
                boolean directoriesCreated = directory.mkdirs();
                if (!directoriesCreated) {
                    System.out.println("Couldn't create directories");
                    return;
                }
            }

            try {
                FileOutputStream fileOutputStream = new FileOutputStream(fullPath);
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
                bufferedOutputStream.write(errorCorrectionResponse.getChunk());

            } catch (IOException ioException) {
                removeChunk(chunk);
                ioException.printStackTrace();
            }

            fixedTampering.set(true);

        } else if (event instanceof FailedNodeHelperRequest) {

            System.out.println("Got FailedNodeHelperRequest!");
            FailedNodeHelperRequest failedNodeHelperRequest = (FailedNodeHelperRequest) event;
            Chunk chunkToReplicate = null;
            for (Chunk chunk : this.chunks) {
                if (chunk.getFileName().equals(failedNodeHelperRequest.getFileName())
                && (chunk.getChunkNumber() == failedNodeHelperRequest.getChunkNumber())) {
                    chunkToReplicate = chunk;
                }
            }

            StoreChunkRequest storeChunkRequest = new StoreChunkRequest(failedNodeHelperRequest.getFileName(), failedNodeHelperRequest.getChunkNumber(), chunkToReplicate.getChunk().length, chunkToReplicate.getChunk(), 0, List.of());
            ChunkServer destinationChunkServer = chunkServerDestring(failedNodeHelperRequest.getChunkServerToStoreReplication());
            TCPChannel destinationChunkServerConnection = getOrCreateConnection(destinationChunkServer.getIpAddress(), destinationChunkServer.getPortNumber());
            destinationChunkServerConnection.getTcpSenderThread().sendData(storeChunkRequest.getbytes());

        } else if (event instanceof RetrieveChunkRequest) {

            System.out.println("Got RetrieveChunkRequest!");
            RetrieveChunkRequest retrieveChunkRequest = (RetrieveChunkRequest) event;
            //todo will be /tmp later
            String directoryPath = "tmp/chunk-server/project/" + this.getIpAddress() + "/";
            String chunkFilePath = String.format("%s_chunk%d", retrieveChunkRequest.getFileName(), retrieveChunkRequest.getChunkNumber());
            String fullPath = directoryPath + chunkFilePath;

            byte[] chunkBytes = Files.readAllBytes(Paths.get(fullPath));
            Integer chunkTamperedWith = verifyChecksum(chunkBytes, retrieveChunkRequest.getFileName(), retrieveChunkRequest.getChunkNumber());

            if (chunkTamperedWith != null) {
                Chunk brokenChunk = null;
                for (Chunk chunk : this.chunks) {
                    if (chunk.getFileName().equals(retrieveChunkRequest.getFileName())
                    && (chunk.getChunkNumber() == retrieveChunkRequest.getChunkNumber())) {
                        brokenChunk = chunk;
                        break;
                    }
                }

                removeChunk(brokenChunk);

                ErrorCorrectionRequest errorCorrectionRequest = new ErrorCorrectionRequest(retrieveChunkRequest.getFileName(), retrieveChunkRequest.getChunkNumber(), this.toString());
                TCPChannel controllerConnection = getOrCreateConnection(this.controllerIPAddress, this.controllerPortNumber);
                controllerConnection.getTcpSenderThread().sendData(errorCorrectionRequest.getbytes());

                while (!fixedTampering.get()) {
                    Thread.sleep(50);
                }

                fixedTampering.set(false);

                //re-read the bytes, cuz now they're fixed
                System.out.println("Tampering solved... rereading bytes");
                chunkBytes = Files.readAllBytes(Paths.get(fullPath));

            }


            RetrieveChunkResponse retrieveChunkResponse = new RetrieveChunkResponse(retrieveChunkRequest.getFileName(), retrieveChunkRequest.getChunkNumber(),
                    chunkBytes.length, chunkBytes);
            System.out.println("Sending RetrieveChunkResponse back to client");
            tcpChannel.getTcpSenderThread().sendData(retrieveChunkResponse.getbytes());

        } else if (event instanceof StoreChunkRequest) {

            StoreChunkRequest storeChunkRequest = (StoreChunkRequest) event;
            System.out.println("Got StoreChunkRequest! Chunk as bytes: " + storeChunkRequest.getChunk());

            //todo remove the "this.get ip address" and the last slash
            String directoryPath = "tmp/chunk-server/project/" + this.getIpAddress() + "/";
            String chunkFilePath = String.format("%s_chunk%d", storeChunkRequest.getFileName(), storeChunkRequest.getChunkNumber());
            String fullPath = directoryPath + chunkFilePath;

            Map<Integer, byte[]> chunkSliceChecksums = computeChunkChecksums(storeChunkRequest.getChunk());

            Chunk chunk = new Chunk(storeChunkRequest.getFileName(), fullPath, this.toString(), storeChunkRequest.getChunkNumber(), storeChunkRequest.getChunk(), chunkSliceChecksums);
            addChunk(chunk);

            //and actually save the chunk
            File directory = new File(directoryPath);
            if (!directory.exists()) {
                boolean directoriesCreated = directory.mkdirs();
                if (!directoriesCreated) {
                    System.out.println("Couldn't create directories");
                    return;
                }
            }

           try {
               FileOutputStream fileOutputStream = new FileOutputStream(fullPath);
               BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
               bufferedOutputStream.write(storeChunkRequest.getChunk());

           } catch (IOException ioException) {
               removeChunk(chunk);
               ioException.printStackTrace();
           }

           //update the controller with knowledge of which chunk this chunk server is now holding
           StoreChunkUpdate storeChunkUpdate = new StoreChunkUpdate(chunk.getFileName(), chunk.getChunkFilePath(), this.toString(), chunk.getChunkNumber());
           TCPChannel controllerConnection = getOrCreateConnection(this.controllerIPAddress, this.controllerPortNumber);
           controllerConnection.getTcpSenderThread().sendData(storeChunkUpdate.getbytes());

            //then possibly forward
            if (storeChunkRequest.getNumLeftSequence() > 0) {
                List<String> leftoverChunksToSendTo = storeChunkRequest.getOtherChunkServersForRedundancy();
                String nextChunkServer = leftoverChunksToSendTo.get(0);
                leftoverChunksToSendTo.remove(0);
                int numLeft = storeChunkRequest.getNumLeftSequence();
                numLeft--;


                StoreChunkRequest newStoreChunkRequest = new StoreChunkRequest(storeChunkRequest.getFileName(), storeChunkRequest.getChunkNumber(),
                        storeChunkRequest.getChunkLength(), storeChunkRequest.getChunk(), numLeft, leftoverChunksToSendTo);
                ChunkServer nextChunkServerToSendChunk = chunkServerDestring(nextChunkServer);

                System.out.println("Sending chunk bytes: " + newStoreChunkRequest.getChunk());
                TCPChannel chunkServerConnection = getOrCreateConnection(nextChunkServerToSendChunk.getIpAddress(), nextChunkServerToSendChunk.getPortNumber());
                chunkServerConnection.getTcpSenderThread().sendData(newStoreChunkRequest.getbytes());

            }

        } else if (event instanceof UpdateChunkServerRequest) {

            UpdateChunkServerRequest updateChunkServerRequest = (UpdateChunkServerRequest) event;
            addOtherChunkServers(List.of(updateChunkServerRequest.getChunkServer()));


        }
    }

    private Integer verifyChecksum(byte[] retrievedSuspectBytes, String requestedFileName, int requestedChunkNumber) throws NoSuchAlgorithmException {
        Chunk desiredChunk = null;
        for (Chunk chunk : this.chunks) {
            if (chunk.getFileName().equals(requestedFileName) && chunk.getChunkNumber() == requestedChunkNumber) {
                desiredChunk = chunk;
            }
        }

        Map<Integer, byte[]> legitimateChecksums = desiredChunk.getChunkSliceChecksums();
        Map<Integer, byte[]> suspectChecksums = computeChunkChecksums(retrievedSuspectBytes);

        for (Map.Entry<Integer, byte[]> legitChecksumEntry : legitimateChecksums.entrySet()) {
            Integer chunkNumber = legitChecksumEntry.getKey();
            byte[] suspectChecksum = suspectChecksums.get(chunkNumber);

            if (!Arrays.equals(suspectChecksum, legitChecksumEntry.getValue())) {
                System.out.println("Slice has been tampered with");
                return chunkNumber;
            }
        }
        return null;

    }

    private Map<Integer, byte[]> computeChunkChecksums(byte[] chunk) throws NoSuchAlgorithmException {
        //8 kilobytes
        int sliceSize = 8192;

        Map<Integer, byte[]> slices = new HashMap<>();

        MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
//        int numberOfSlices = (int) Math.ceil((double) chunk.length / sliceSize);

        int start = 0;
        int sliceIndex = 0;
        while (start < chunk.length) {
            int end = Math.min(start + sliceSize, chunk.length);
            byte[] chunkSlice = new byte[end - start];
            System.arraycopy(chunk, start, chunkSlice, 0, chunkSlice.length);

            messageDigest.reset();
            messageDigest.update(chunkSlice);
            byte[] chunkSliceChecksum = messageDigest.digest();

            slices.put(sliceIndex, chunkSliceChecksum);
            sliceIndex++;
            start += sliceSize;
        }

        return slices;
    }

    private synchronized void addChunk(Chunk chunk) {
        this.chunks.add(chunk);
        this.newlyAddedChunks.add(chunk);
    }

    private synchronized void removeChunk(Chunk chunk) {
        this.chunks.remove(chunk);
    }

    private synchronized void addOtherChunkServers(List<String> otherChunkServers) {
        this.otherChunkServers.addAll(otherChunkServers);
    }

    private void registerWithController() throws IOException, InterruptedException {

        Socket socketToHost = new Socket(this.controllerIPAddress, this.controllerPortNumber);
        TCPChannel tcpChannel = new TCPChannel(socketToHost, this);

        Thread receivingThread = new Thread(tcpChannel.getTcpReceiverThread());
        receivingThread.start();

        Thread sendingThread = new Thread(tcpChannel.getTcpSenderThread());
        sendingThread.start();

        controllerTcpChannel = tcpChannel;

        ChunkServerRegisterRequest chunkServerRegisterRequest = new ChunkServerRegisterRequest(this.getIpAddress(), this.getPortNumber());
        controllerTcpChannel.getTcpSenderThread().sendData(chunkServerRegisterRequest.getbytes());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        ChunkServer controllerObject = (ChunkServer) object;
        return portNumber == controllerObject.portNumber && Objects.equals(ipAddress, controllerObject.ipAddress);
    }

    private ChunkServer chunkServerDestring(String chunkServerAsString) {
        String[] ipAddressAndPort = chunkServerAsString.split(":");
        String ipAddress = ipAddressAndPort[0];
        int portNumber = Integer.parseInt(ipAddressAndPort[1]);
        return new ChunkServer(ipAddress, portNumber);
    }

    @Override
    public String toString() {
        return ipAddress + ":" + portNumber;
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
}
