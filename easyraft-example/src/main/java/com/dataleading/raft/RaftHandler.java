package com.dataleading.raft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONObject;
import com.dataleading.raft.RaftMessage.AppendEntriesRequest;
import com.dataleading.raft.RaftMessage.AppendEntriesResponse;
import com.dataleading.raft.RaftMessage.VoteRequest;
import com.dataleading.raft.RaftMessage.VoteResponse;
import com.sun.net.httpserver.*;

@SuppressWarnings("restriction")
public class RaftHandler implements HttpHandler {

  private static Logger logger = Logger.getLogger(RaftHandler.class.getName());
  private RaftNode raftNode;
  private final String defaultResp = "{\"error\":\"internal error happens\"}\n";

  public RaftHandler(RaftNode raftNode) {
    this.raftNode = raftNode;
  }

  @Override
  public void handle(HttpExchange t) throws IOException {
    InputStreamReader isr = new InputStreamReader(t.getRequestBody(), "utf-8");
    BufferedReader br = new BufferedReader(isr);
    int b;
    StringBuilder buf = new StringBuilder(512);
    while ((b = br.read()) != -1) {
      buf.append((char) b);
    }
    br.close();
    isr.close();

    String resp = defaultResp;
    String q = t.getRequestURI().getQuery();
    logger.log(Level.FINE, "request query is {0}", q);

    Map<String, String> paramMap = queryToMap(q);
    String type = paramMap.get("type");

    try {
      if ("vote".equals(type)) {
        JSONObject req = new JSONObject(buf.toString());
        resp = voteRequest(req);
      } else if ("appendEntries".equals(type)) {
        JSONObject req = new JSONObject(buf.toString());
        resp = appendEntries(req);
      } else if ("write".equals(type)) {
        resp = raftNode.writeService(buf.toString());
      } else if ("query".equals(type)) {
        StateMachine s = raftNode.getStateMachine();
        String key = paramMap.get("key");
        if(key!=null) {
          String v = s.query(key);
          if(v != null ) {
            resp = new JSONObject().put(key, v).toString() + "\n";
          }
        }       
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, e.getMessage(), e);
    }

    logger.log(Level.FINE, "handler response {0}", resp);

    byte response[] = resp.getBytes("UTF-8");
    t.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
    t.sendResponseHeaders(200, response.length); // response code and length
    OutputStream os = t.getResponseBody();
    os.write(response);
    os.close();
  }

  private String voteRequest(JSONObject jMessage) {
    VoteRequest voteReq = new VoteRequest(jMessage);
    VoteResponse voteResp = raftNode.voteService(voteReq);
    return voteResp.toJsonText();
  }

  private String appendEntries(JSONObject jMessage) {
    AppendEntriesRequest appendReq = new AppendEntriesRequest(jMessage);
    AppendEntriesResponse appendResp = raftNode.appendEntriesService(appendReq);
    return appendResp.toJsonText();
  }

  private Map<String, String> queryToMap(String query) {
    Map<String, String> result = new HashMap<String, String>();
    for (String param : query.split("&")) {
      String pair[] = param.split("=");
      if (pair.length > 1) {
        result.put(pair[0], pair[1]);
      } else {
        result.put(pair[0], "");
      }
    }
    return result;
  }

}
