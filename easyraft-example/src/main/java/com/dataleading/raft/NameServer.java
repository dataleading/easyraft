
package com.dataleading.raft;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.sun.net.httpserver.*;

@SuppressWarnings("restriction")
public class NameServer {
  
  private static Logger logger = Logger.getLogger(NameServer.class.getName());

  public static void main(String[] args) {
    NameServer server = new NameServer();
    OptionGroup options = new OptionGroup().enableNameServer();
    int listenPort = 8080;
    try {
      CommandParser parser = new CommandParser(options, args);
      String cmd = parser.getCommand();
      if ("start".equals(cmd)) {
        String peers = parser.getOptionValue("peers");
        String port = parser.getOptionValue("port");
        String level = parser.getOptionValue("level");
        listenPort = Integer.parseInt(port);
        Set<PeerServer> peerServers = server.parsePeers(peers);
        if ("debug".equals(level)) {
          SimpleLogger.getStdInstance().setLevel(Level.FINER);
        }else {
          SimpleLogger.getStdInstance().setLevel(Level.INFO);
        }

        StateMachine stateMachine = new KVStateMachine();

        RaftNode raftNode =
            new RaftNode(peerServers, new EndPoint("localhost", listenPort), stateMachine);
        server.setupRaftEndpoint(listenPort, raftNode);
      }
    } catch (BindException e) {
      logger.log(Level.SEVERE, "failed to listen port:{0}", String.valueOf(listenPort));
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
      options.printHelp();
    } catch (Exception e) {
      logger.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  private void setupRaftEndpoint(int port, RaftNode raftNode) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.setExecutor(Executors.newCachedThreadPool());
    
    HttpContext context = server.createContext("/raft");
    context.setHandler(new RaftHandler(raftNode));
    server.start();
  }

  private Set<PeerServer> parsePeers(String peers) {
    Set<PeerServer> servers = new HashSet<PeerServer>();
    String[] ps = peers.split(",");
    try {
      for (String s : ps) {
        String[] ep = s.split(":");
        if (ep.length == 2) {
          String hostname = ep[0];
          int port = Integer.parseInt(ep[1]);
          EndPoint ee = new EndPoint(hostname, port);
          PeerServer pp = new PeerServer(ee);
          servers.add(pp);
        }
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("illegal peer server argument value");
    }

    if (servers.size() + 1 != 3) {
      throw new IllegalArgumentException("total peer servers must be 3");
    }
    return servers;
  }
}
