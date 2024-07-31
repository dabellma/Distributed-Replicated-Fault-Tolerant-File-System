package csx55.dfs.util;

import java.util.Map;

public class Chunk {

    //I think this chunk constructor can eventually be deleted
    //it may help with debugging which chunks, as bytes, are owned by which chunk server,
    //so i'll keep it around for now. But later on, it shouldn't need to save
    //the byte array of a chunk, and should "only" read it from the known chunkfilepath when needed
    public Chunk(String fileName, String chunkFilePath, String chunkServer, int chunkNumber, byte[] chunk, Map<Integer, byte[]> chunkSliceChecksums) {
        this.fileName = fileName;
        this.chunkFilePath = chunkFilePath;
        this.chunkServer = chunkServer;
        this.chunkNumber = chunkNumber;
        this.chunk = chunk;
        this.chunkSliceChecksums = chunkSliceChecksums;
    }

    public Chunk(String fileName, String chunkFilePath, String chunkServer, int chunkNumber) {
        this.fileName = fileName;
        this.chunkFilePath = chunkFilePath;
        this.chunkServer = chunkServer;
        this.chunkNumber = chunkNumber;
        this.chunk = null;
        this.chunkSliceChecksums = null;
    }

    private String fileName;
    private String chunkFilePath;
    private String chunkServer;
    private int chunkNumber;
    private byte[] chunk;
    private Map<Integer, byte[]> chunkSliceChecksums;

    public String getFileName() {
        return fileName;
    }

    public String getChunkFilePath() {
        return chunkFilePath;
    }

    public String getChunkServer() {
        return chunkServer;
    }

    public int getChunkNumber() {
        return chunkNumber;
    }

    public byte[] getChunk() {
        return chunk;
    }

    public Map<Integer, byte[]> getChunkSliceChecksums() {
        return chunkSliceChecksums;
    }
}
