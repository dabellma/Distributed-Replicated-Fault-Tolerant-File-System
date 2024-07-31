package csx55.dfs;



import csx55.dfs.transport.TCPChannel;
import csx55.dfs.wireformats.Event;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public interface Node {
    void onEvent(Event event, TCPChannel tcpChannel) throws IOException, InterruptedException, NoSuchAlgorithmException;
}
