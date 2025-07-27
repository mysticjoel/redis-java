import java.io.*;
import java.net.Socket;
import java.util.*;

public class ReplicaClient {
    private final Map<String, String> config;
    private final KeyValueStore kvStore;
    private final ReplicationManager replicationManager;

    public ReplicaClient(Map<String, String> config, KeyValueStore kvStore, ReplicationManager replicationManager) {
        this.config = config;
        this.kvStore = kvStore;
        this.replicationManager = replicationManager;
    }

    public void connect() {
        try {
            String[] parts = config.get("replicaof").split(" ");
            String masterHost = parts[0];
            int masterPort = Integer.parseInt(parts[1]);
            Socket socket = new Socket(masterHost, masterPort);
            ReplicaConnection connection = new ReplicaConnection(socket);

            OutputStream out = connection.getOutputStream();
            InputStream in = connection.getInputStream();

            // Handshake
            out.write(RESPParser.buildArray("PING").getBytes());
            out.flush();
            in.read(new byte[10000]);

            out.write(RESPParser.buildArray("REPLCONF", "listening-port", config.get("port")).getBytes());
            out.flush();
            in.read(new byte[10000]);

            out.write(RESPParser.buildArray("REPLCONF", "capa", "psync2").getBytes());
            out.flush();
            in.read(new byte[10000]);

            out.write(RESPParser.buildArray("PSYNC", "?", "-1").getBytes());
            out.flush();

            PushbackInputStream pin = new PushbackInputStream(in);
            skipUntilStar(pin);

            // Main replication loop
            while (true) {
                RESPParser.ParseResult result = RESPParser.parseRESP(pin);
                List<String> command = result.command;
                if (command.isEmpty()) continue;

                String cmd = command.get(0).toUpperCase();
                switch (cmd) {
                    case "PING":
                        connection.setOffset(connection.getOffset() + result.bytesConsumed);
                        break;
                    case "SET":
                        kvStore.set(command.get(1), command.get(2), Long.MAX_VALUE);
                        connection.setOffset(connection.getOffset() + result.bytesConsumed);
                        break;
                    case "REPLCONF":
                        String response = RESPParser.buildArray("REPLCONF", "ACK",
                                String.valueOf(connection.getOffset()));
                        connection.setOffset(connection.getOffset() + result.bytesConsumed);
                        out.write(response.getBytes());
                        out.flush();
                        break;
                    default:
                        System.out.println("Unknown command: " + cmd);
                }
            }
        } catch (IOException e) {
            System.out.println("Replica connection error: " + e.getMessage());
        }
    }

    private void skipUntilStar(PushbackInputStream pin) throws IOException {
        int b;
        while ((b = pin.read()) != -1) {
            if (b == '*') {
                pin.unread(b);
                return;
            }
        }
        throw new EOFException("Reached end of stream before finding '*'");
    }
}