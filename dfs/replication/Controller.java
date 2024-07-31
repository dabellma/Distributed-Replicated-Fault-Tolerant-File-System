package csx55.dfs.replication;


import csx55.dfs.Node;
import csx55.dfs.transport.TCPChannel;
import csx55.dfs.transport.TCPServerThread;
import csx55.dfs.util.Chunk;
import csx55.dfs.wireformats.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Controller implements Node {

    private Map<String, TCPChannel> tcpChannels = new ConcurrentHashMap<>();
    private Map<String, Long> allChunkServersMap = new ConcurrentHashMap<>();

    //bytes are not stored in this chunks list
    //the controller does not pass data to the client, but does give the client information
    //on which chunk servers to contact to recreate a file
    private List<Chunk> chunks = new ArrayList<>();
    public Map<String, TCPChannel> getTCPChannels() {
        return this.tcpChannels;
    }

    public static void main(String[] args) {

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        if (args.length == 1) {
            try {
                Controller controller = new Controller();

                System.out.println("Started controller");
                ServerSocket ourServerSocket = new ServerSocket(Integer.parseInt(args[0]));

                Thread controllerServerThread = new Thread(new TCPServerThread(ourServerSocket, controller));
                controllerServerThread.start();

                while (true) {
                    String input;
                    input = reader.readLine();

                    controller.processInput(input);
                }

            } catch (IOException | InterruptedException exception) {
                System.out.println("Encountered an issue while setting up main discovery");
                System.exit(1);
            }

        } else {
            System.out.println("Please enter exactly one argument. Exiting.");
            System.exit(1);
        }
    }

    private void processInput(String input) throws IOException, InterruptedException {
        String[] tokens = input.split("\\s+");
        switch (tokens[0].toUpperCase()) {

            case "CONNECTIONS":
                for (String peer : tcpChannels.keySet()) {
                    System.out.println(peer);
                }
                break;
            case "CHUNKS":
                for (Chunk chunk : this.chunks) {
                    System.out.println(chunk.getFileName() + " " + chunk.getChunkFilePath() + " " + chunk.getChunkNumber());
                }
                break;
            case "CHUNK-SERVERS":
                for (String chunkServer : getAllChunkServersMap().keySet()) {
                    System.out.println(chunkServer);
                }
                break;
            default:
                System.out.println("Unknown command. Re-entering wait period.");
                break;
        }
    }

    @Override
    public void onEvent(Event event, TCPChannel tcpChannel) throws IOException, InterruptedException {
        if (event instanceof ChunkServerRegisterRequest) {
            ChunkServerRegisterRequest chunkServerRegisterRequest = (ChunkServerRegisterRequest) event;

            //notify other chunk servers of the joined chunk server
            //note: this is done before the new chunk server is added to the list of chunk servers
            for (String chunkServer : getAllChunkServersMap().keySet()) {
                ChunkServer chunkServerObject = chunkServerDestring(chunkServer);
                TCPChannel existingChunkServerChannel = getOrCreateConnection(chunkServerObject.getIpAddress(), chunkServerObject.getPortNumber());

                UpdateChunkServerRequest updateChunkServerRequest = new UpdateChunkServerRequest(new ChunkServer(chunkServerRegisterRequest.getIpAddress(), chunkServerRegisterRequest.getPortNumber()).toString());
                existingChunkServerChannel.getTcpSenderThread().sendData(updateChunkServerRequest.getbytes());
            }

            TCPChannel joiningChunkServerChannel = getOrCreateConnection(chunkServerRegisterRequest.getIpAddress(), chunkServerRegisterRequest.getPortNumber());

            List<String> allChunkServers = new ArrayList<>(getAllChunkServersMap().keySet());
            ChunkServerRegisterResponse chunkServerRegisterResponse = new ChunkServerRegisterResponse(allChunkServers.size(), allChunkServers);
            joiningChunkServerChannel.getTcpSenderThread().sendData(chunkServerRegisterResponse.getbytes());
            addToChunkServersMap(new ChunkServer(chunkServerRegisterRequest.getIpAddress(), chunkServerRegisterRequest.getPortNumber()).toString(), Instant.now().toEpochMilli());

        } else if (event instanceof ClientControllerChunkRetrievalRequest) {

            ClientControllerChunkRetrievalRequest clientControllerChunkRetrievalRequest = (ClientControllerChunkRetrievalRequest) event;

            List<Chunk> chunksToRecreateFile = getChunksForFile(clientControllerChunkRetrievalRequest.getFile());

            ClientControllerChunkRetrievalResponse clientControllerChunkRetrievalResponse = new ClientControllerChunkRetrievalResponse(chunksToRecreateFile.size(), chunksToRecreateFile);
            tcpChannel.getTcpSenderThread().sendData(clientControllerChunkRetrievalResponse.getbytes());

        } else if (event instanceof ClientControllerChunkStorageRequest) {
            ClientControllerChunkStorageRequest clientControllerChunkStorageRequest = (ClientControllerChunkStorageRequest) event;

            if (getAllChunkServersMap().size() < 3) {
                ClientControllerChunkStorageResponse clientControllerChunkStorageResponse = new ClientControllerChunkStorageResponse(List.of(), false);
                tcpChannel.getTcpSenderThread().sendData(clientControllerChunkStorageResponse.getbytes());
            } else {
                List<String> chunkServersToStoreChunks = getThreeRandomChunkServers();
                ClientControllerChunkStorageResponse clientControllerChunkStorageResponse = new ClientControllerChunkStorageResponse(chunkServersToStoreChunks, true);
                tcpChannel.getTcpSenderThread().sendData(clientControllerChunkStorageResponse.getbytes());
            }

        } else if (event instanceof ClientRegisterRequest) {
            ClientRegisterRequest clientRegisterRequest = (ClientRegisterRequest) event;

            TCPChannel joiningClientChannel = getOrCreateConnection(clientRegisterRequest.getIpAddress(), clientRegisterRequest.getPortNumber());

            ClientRegisterResponse clientRegisterResponse = new ClientRegisterResponse();
            joiningClientChannel.getTcpSenderThread().sendData(clientRegisterResponse.getbytes());


        } else if (event instanceof ErrorCorrectionRequest) {

            ErrorCorrectionRequest errorCorrectionRequest = (ErrorCorrectionRequest) event;
            Chunk helperChunkServer = null;
            for (Chunk chunk : this.chunks) {
                if (chunk.getFileName().equals(errorCorrectionRequest.getFileName())
                        && (chunk.getChunkNumber() == errorCorrectionRequest.getChunkNumber())
                        && !chunk.getChunkServer().equals(errorCorrectionRequest.getBrokenChunkServer())) {
                    helperChunkServer = chunk;
                    break;
                }
            }

            ChunkServer helperChunkServerReal = chunkServerDestring(helperChunkServer.getChunkServer());
            ErrorCorrectionForward errorCorrectionForward = new ErrorCorrectionForward(errorCorrectionRequest.getFileName(), errorCorrectionRequest.getChunkNumber(), errorCorrectionRequest.getBrokenChunkServer());
            TCPChannel helperChunkServerConnection = getOrCreateConnection(helperChunkServerReal.getIpAddress(), helperChunkServerReal.getPortNumber());
            helperChunkServerConnection.getTcpSenderThread().sendData(errorCorrectionForward.getbytes());

        //todo make minor and major heartbeat call a common method
        } else if (event instanceof MinorHeartbeat) {
            MinorHeartbeat minorHeartbeat = (MinorHeartbeat) event;
            addToChunkServersMap(minorHeartbeat.getSendingNode(), Instant.now().toEpochMilli());
            System.out.println("Got minor heartbeat");
            for (Map.Entry<String, Long> entry : getAllChunkServersMap().entrySet()) {
                boolean isFailed = isFailedNode(entry.getValue());
                if (isFailed) {
                    System.out.println("Failed node: " + entry.getKey());
                    if (this.allChunkServersMap.size() < 3) {
                        System.out.println("Not enough nodes to handle 3-fold replication since 1 of 3 nodes have failed, leaving only 2. System error.");
                        break;
                    }
                    List<Chunk> chunksNeedingReplicationAfterFailedChunkServer = new ArrayList<>();
                    String failedChunkServer = entry.getKey();
                    for (Chunk chunk : this.chunks) {
                        if (chunk.getChunkServer().equals(failedChunkServer)) {
                            chunksNeedingReplicationAfterFailedChunkServer.add(chunk);
                        }
                    }

                    //remove them from the controller's known list now, since the node is failed
                    this.chunks.removeAll(chunksNeedingReplicationAfterFailedChunkServer);
                    removeFromChunkServersMap(failedChunkServer);
                    tcpChannels.remove(failedChunkServer);


                    for (Chunk chunkNeedingReplication : chunksNeedingReplicationAfterFailedChunkServer) {
                        List<String> chunkServersThatHoldChunkAlready = new ArrayList<>();
                        List<String> chunkServersThatDoNotHoldChunkAlready = new ArrayList<>();
                        System.out.println("Chunk needing replication: " + chunkNeedingReplication.getFileName() + " " + chunkNeedingReplication.getChunkNumber());
                        for (Chunk knownChunk : this.chunks) {
                            if (knownChunk.getFileName().equals(chunkNeedingReplication.getFileName())
                            && knownChunk.getChunkNumber() == chunkNeedingReplication.getChunkNumber()) {
                                chunkServersThatHoldChunkAlready.add(knownChunk.getChunkServer());
                            }
                        }

                        for (String chunkServer : this.allChunkServersMap.keySet()) {
                            if (!chunkServersThatHoldChunkAlready.contains(chunkServer)) {
                                chunkServersThatDoNotHoldChunkAlready.add(chunkServer);
                            }
                        }

                        System.out.println("Chunk servers hold chunk already: " + chunkServersThatHoldChunkAlready);
                        System.out.println("Chunk servers do not hold chunk already: " + chunkServersThatDoNotHoldChunkAlready);
                        String helperChunkServerString = chunkServersThatHoldChunkAlready.get(0);
                        String chunkServerToStoreReplicationString = chunkServersThatDoNotHoldChunkAlready.get(0);
                        System.out.println("Helper chunk: " + helperChunkServerString);
                        System.out.println("To store replication chunk: " + chunkServerToStoreReplicationString);
                        ChunkServer helperChunkServer = chunkServerDestring(helperChunkServerString);
                        ChunkServer chunkServerToStoreReplication = chunkServerDestring(chunkServerToStoreReplicationString);

                        FailedNodeHelperRequest failedNodeHelperRequest = new FailedNodeHelperRequest(chunkNeedingReplication.getFileName(), chunkNeedingReplication.getChunkFilePath(), chunkServerToStoreReplication.toString(), chunkNeedingReplication.getChunkNumber());
                        TCPChannel helperChunkServerConnection = getOrCreateConnection(helperChunkServer.getIpAddress(), helperChunkServer.getPortNumber());
                        helperChunkServerConnection.getTcpSenderThread().sendData(failedNodeHelperRequest.getbytes());


                    }

                }
            }

        } else if (event instanceof MajorHeartbeat) {
            MajorHeartbeat majorHeartbeat = (MajorHeartbeat) event;
            addToChunkServersMap(majorHeartbeat.getSendingNode(), Instant.now().toEpochMilli());
            System.out.println("Got major heartbeat");
            for (Map.Entry<String, Long> entry : getAllChunkServersMap().entrySet()) {
                boolean isFailed = isFailedNode(entry.getValue());
                if (isFailed) {
                    System.out.println("Failed node: " + entry.getKey());
                    if (this.allChunkServersMap.size() < 3) {
                        System.out.println("Not enough nodes to handle 3-fold replication since 1 of 3 nodes have failed, leaving only 2. System error.");
                        break;
                    }
                    List<Chunk> chunksNeedingReplicationAfterFailedChunkServer = new ArrayList<>();
                    String failedChunkServer = entry.getKey();
                    for (Chunk chunk : this.chunks) {
                        if (chunk.getChunkServer().equals(failedChunkServer)) {
                            chunksNeedingReplicationAfterFailedChunkServer.add(chunk);
                        }
                    }

                    //remove them from the controller's known list now, since the node is failed
                    this.chunks.removeAll(chunksNeedingReplicationAfterFailedChunkServer);
                    removeFromChunkServersMap(failedChunkServer);
                    tcpChannels.remove(failedChunkServer);


                    for (Chunk chunkNeedingReplication : chunksNeedingReplicationAfterFailedChunkServer) {
                        List<String> chunkServersThatHoldChunkAlready = new ArrayList<>();
                        List<String> chunkServersThatDoNotHoldChunkAlready = new ArrayList<>();
                        System.out.println("Chunk needing replication: " + chunkNeedingReplication.getFileName() + " " + chunkNeedingReplication.getChunkNumber());
                        for (Chunk knownChunk : this.chunks) {
                            if (knownChunk.getFileName().equals(chunkNeedingReplication.getFileName())
                                    && knownChunk.getChunkNumber() == chunkNeedingReplication.getChunkNumber()) {
                                chunkServersThatHoldChunkAlready.add(knownChunk.getChunkServer());
                            }
                        }

                        for (String chunkServer : this.allChunkServersMap.keySet()) {
                            if (!chunkServersThatHoldChunkAlready.contains(chunkServer)) {
                                chunkServersThatDoNotHoldChunkAlready.add(chunkServer);
                            }
                        }

                        System.out.println("Chunk servers hold chunk already: " + chunkServersThatHoldChunkAlready);
                        System.out.println("Chunk servers do not hold chunk already: " + chunkServersThatDoNotHoldChunkAlready);
                        String helperChunkServerString = chunkServersThatHoldChunkAlready.get(0);
                        String chunkServerToStoreReplicationString = chunkServersThatDoNotHoldChunkAlready.get(0);
                        System.out.println("Helper chunk: " + helperChunkServerString);
                        System.out.println("To store replication chunk: " + chunkServerToStoreReplicationString);
                        ChunkServer helperChunkServer = chunkServerDestring(helperChunkServerString);
                        ChunkServer chunkServerToStoreReplication = chunkServerDestring(chunkServerToStoreReplicationString);

                        FailedNodeHelperRequest failedNodeHelperRequest = new FailedNodeHelperRequest(chunkNeedingReplication.getFileName(), chunkNeedingReplication.getChunkFilePath(), chunkServerToStoreReplication.toString(), chunkNeedingReplication.getChunkNumber());
                        TCPChannel helperChunkServerConnection = getOrCreateConnection(helperChunkServer.getIpAddress(), helperChunkServer.getPortNumber());
                        helperChunkServerConnection.getTcpSenderThread().sendData(failedNodeHelperRequest.getbytes());


                    }

                }
            }


        } else if (event instanceof StoreChunkUpdate) {
            StoreChunkUpdate storeChunkUpdate = (StoreChunkUpdate) event;
            Chunk chunk = new Chunk(storeChunkUpdate.getFileName(), storeChunkUpdate.getChunkFilePath(), storeChunkUpdate.getChunkServer(), storeChunkUpdate.getChunkNumber());
            addChunk(chunk);

        } else {
            System.out.println("Received unknown event");
        }
    }

    private List<Chunk> getChunksForFile(String file) {
        List<Chunk> finalChunksToRecreateFile = new ArrayList<>();
        Set<Chunk> possibleChunksForFile = new HashSet<>();
        for (Chunk chunk : this.chunks) {
            if (chunk.getFileName().equals(file)) {
                possibleChunksForFile.add(chunk);
            }
        }

        Set<Integer> allChunkPartNumbers = new HashSet<>();
        for (Chunk chunk : possibleChunksForFile) {
            allChunkPartNumbers.add(chunk.getChunkNumber());
        }

        outerLoop:
        for (Integer chunkPartNumber : allChunkPartNumbers) {
            for (Chunk chunk : possibleChunksForFile) {
                if (chunk.getChunkNumber() == chunkPartNumber) {
                    finalChunksToRecreateFile.add(chunk);
                    continue outerLoop;
                }
            }
        }

        //quick check for sizing errors
        if (finalChunksToRecreateFile.size() != allChunkPartNumbers.size()) {
            System.out.println("ERROR: too many chunks returned from controller for a given file");
            finalChunksToRecreateFile = new ArrayList<>();
        }

        return finalChunksToRecreateFile;
    }

    private List<String> getThreeRandomChunkServers() {
        Random random = new Random();
        List<String> threeRandomChunkServers = new ArrayList<>();
        Set<Integer> randomIntegers = new HashSet<>();
        while (randomIntegers.size() < 3) {
            randomIntegers.add(random.nextInt(getAllChunkServersMap().size()));
        }
        for (Integer randomInteger : randomIntegers) {
            List<String> allChunkServers = new ArrayList<>(getAllChunkServersMap().keySet());
            threeRandomChunkServers.add(allChunkServers.get(randomInteger));
        }
        return threeRandomChunkServers;
    }

    private synchronized void addChunk(Chunk chunk) {
        this.chunks.add(chunk);
    }


    private ChunkServer chunkServerDestring(String chunkServerAsString) {
        String[] ipAddressAndPort = chunkServerAsString.split(":");
        String ipAddress = ipAddressAndPort[0];
        int portNumber = Integer.parseInt(ipAddressAndPort[1]);
        return new ChunkServer(ipAddress, portNumber);
    }

    private boolean isFailedNode(Long lastUpdate) {
        Long timeSinceLastUpdate = Instant.now().toEpochMilli() - lastUpdate;
        System.out.println(timeSinceLastUpdate);
        if (timeSinceLastUpdate > 18000) {
            return true;
        }
        return false;
    }

    //these methods are synchronized so a read can't happen at the same time as a write
    private synchronized Map<String, Long> getAllChunkServersMap() {
        return this.allChunkServersMap;
    }

    private synchronized void addToChunkServersMap(String chunkServer, Long time) {
        this.allChunkServersMap.put(chunkServer, time);
    }

    private synchronized void removeFromChunkServersMap(String chunkServer) {
        this.allChunkServersMap.remove(chunkServer);
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
