package csx55.dfs.wireformats;

public enum Protocol {
    CLIENT_REGISTER_REQUEST(0),
    CLIENT_REGISTER_RESPONSE(12),
    CHUNK_SERVER_REGISTER_REQUEST(13),
    CHUNK_SERVER_REGISTER_RESPONSE(14),
    UPDATE_CHUNK_SERVER_REQUEST(15),
    STORE_CHUNK_REQUEST(16),
    CLIENT_CONTROLLER_CHUNK_STORAGE_REQUEST(17),
    CLIENT_CONTROLLER_CHUNK_STORAGE_RESPONSE(18),
    CLIENT_CONTROLLER_CHUNK_RETRIEVAL_REQUEST(19),
    CLIENT_CONTROLLER_CHUNK_RETRIEVAL_RESPONSE(20),
    RETRIEVE_CHUNK_REQUEST(21),
    RETRIEVE_CHUNK_RESPONSE(22),
    STORE_CHUNK_UPDATE(23),
    ERROR_CORRECTION_REQUEST(24),
    ERROR_CORRECTION_FORWARD(25),
    ERROR_CORRECTION_RESPONSE(26),
    MINOR_HEARTBEAT(27),
    MAJOR_HEARTBEAT(28),
    FAILED_NODE_HELPER_REQUEST(29);

    private final int value;

    Protocol(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
