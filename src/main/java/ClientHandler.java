import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    public static final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<String, Long> expiryMap = new ConcurrentHashMap<>();
    byte[] emptyRDB = Base64.getDecoder().decode(
            "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
    );

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
    }

    public void propagateToReplicas(String[] commandParts) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(commandParts.length).append("\r\n");
        for (String part : commandParts) {
            sb.append("$").append(part.length()).append("\r\n").append(part).append("\r\n");
        }
        byte[] payload = sb.toString().getBytes();
        synchronized (Main.class) {
            Main.masterOffset += payload.length;
        }
        synchronized (Main.replicaConnections) {
            for (Main.ReplicaConnection replica : Main.replicaConnections.values()) {
                try {
                    replica.outputStream.write(payload);
                    replica.outputStream.flush();
                    System.out.println("Propagated to replica@" + replica.port + ": " + sb.toString().replace("\r\n", "\\r\\n"));
                } catch (IOException e) {
                    Main.replicaConnections.remove(replica.port);
                    System.err.println("Failed to propagate to replica@" + replica.port + ": " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void run() {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                OutputStream writer = clientSocket.getOutputStream()
        ) {
            String line;
            Integer replicaPort = null;
            while ((line = reader.readLine()) != null) {
                System.out.println("Received line: " + line + ", role: " + (Main.master != null ? "slave" : "master"));

                if (line.trim().equalsIgnoreCase("*2")) {
                    String commandLength = reader.readLine();
                    String command = reader.readLine();
                    System.out.println("Received *2 command: " + command + ", role: " + (Main.master != null ? "slave" : "master"));
                    if (command.equalsIgnoreCase("DEL") && Main.master == null) { // Master only
                        reader.readLine(); // e.g., $<len>
                        String key = reader.readLine(); // e.g., key
                        map.remove(key);
                        expiryMap.remove(key);
                        StringBuilder sb = new StringBuilder();
                        sb.append("*2\r\n$3\r\nDEL\r\n$").append(key.length()).append("\r\n").append(key).append("\r\n");
                        writer.write(":1\r\n".getBytes());
                        propagateToReplicas(new String[]{"DEL", key});
                        writer.flush();
                        continue;
                    }
                }

                if (line.trim().equalsIgnoreCase("*3")) {
                    String commandLength = reader.readLine();
                    String command = reader.readLine();
                    System.out.println("Received *3 command: " + command + ", role: " + (Main.master != null ? "slave" : "master"));
                    if (command.equalsIgnoreCase("WAIT") && Main.master == null) { // Master only
                        reader.readLine(); // e.g., $1
                        String numReplicasStr = reader.readLine(); // e.g., 1
                        reader.readLine(); // e.g., $3
                        String timeoutStr = reader.readLine(); // e.g., 500
                        int numReplicas = Integer.parseInt(numReplicasStr);
                        long timeoutMs = Long.parseLong(timeoutStr);

                        long startTime = System.currentTimeMillis();
                        long targetOffset;
                        synchronized (Main.class) {
                            targetOffset = Main.masterOffset;
                        }
                        int ackCount = 0;
                        Set<Integer> respondingReplicas = new HashSet<>();
                        String getAck = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";

                        while (ackCount < numReplicas && (System.currentTimeMillis() - startTime) < timeoutMs) {
                            synchronized (Main.replicaConnections) {
                                for (Main.ReplicaConnection replica : new ArrayList<>(Main.replicaConnections.values())) {
                                    if (respondingReplicas.contains(replica.port)) continue;
                                    try {
                                        replica.outputStream.write(getAck.getBytes());
                                        replica.outputStream.flush();
                                        System.out.println("Sent REPLCONF GETACK to replica@" + replica.port);
                                        String respLine = replica.inputStream.readLine();
                                        System.out.println("Received from replica@" + replica.port + ": " + respLine);
                                        if (respLine != null && respLine.equals("*3")) {
                                            replica.inputStream.readLine(); // $8
                                            replica.inputStream.readLine(); // REPLCONF
                                            replica.inputStream.readLine(); // $3
                                            replica.inputStream.readLine(); // ACK
                                            replica.inputStream.readLine(); // $<length>
                                            String offsetStr = replica.inputStream.readLine(); // offset
                                            long replicaOffset = Long.parseLong(offsetStr);
                                            System.out.println("Replica@" + replica.port + " ACK offset: " + replicaOffset + ", target: " + targetOffset);
                                            if (replicaOffset >= targetOffset) {
                                                respondingReplicas.add(replica.port);
                                                ackCount++;
                                            }
                                        } else {
                                            System.out.println("Invalid or no ACK from replica@" + replica.port);
                                        }
                                    } catch (IOException e) {
                                        Main.replicaConnections.remove(replica.port);
                                        System.err.println("Error communicating with replica@" + replica.port + ": " + e.getMessage());
                                    }
                                }
                            }
                            if (ackCount < numReplicas) {
                                try {
                                    Thread.sleep(50);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }

                        String response = ":" + ackCount + "\r\n";
                        System.out.println("WAIT response: " + response);
                        writer.write(response.getBytes());
                        writer.flush();
                        continue;
                    } else if (command.equalsIgnoreCase("REPLCONF")) {
                        reader.readLine(); // e.g., $14 or $4 or $6 or $3
                        String subcommand = reader.readLine(); // e.g., listening-port, capa, ACK
                        reader.readLine(); // e.g., $4 or $6 or $2
                        String arg = reader.readLine(); // e.g., 6380, psync2, 31
                        System.out.println("Received REPLCONF subcommand: " + subcommand + ", arg: " + arg + ", role: " + (Main.master != null ? "slave" : "master"));
                        if (subcommand.equalsIgnoreCase("listening-port")) {
                            replicaPort = Integer.parseInt(arg);
                            writer.write("+OK\r\n".getBytes());
                            writer.flush();
                        } else if (subcommand.equalsIgnoreCase("capa")) {
                            writer.write("+OK\r\n".getBytes());
                            writer.flush();
                        } else if (subcommand.equalsIgnoreCase("ACK")) {
                            // Master processes ACK from replicas
                            continue;
                        }
                        continue;
                    } else if (command.equalsIgnoreCase("PSYNC")) {
                        reader.readLine(); // e.g., $1
                        reader.readLine(); // e.g., ?
                        reader.readLine(); // e.g., $2
                        reader.readLine(); // e.g., -1
                        writer.write("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".getBytes());
                        writer.write(("$" + emptyRDB.length + "\r\n").getBytes());
                        writer.write(emptyRDB);
                        if (replicaPort != null) {
                            Main.replicaConnections.put(replicaPort, new Main.ReplicaConnection(clientSocket, replicaPort));
                            System.out.println("Added replica@" + replicaPort + " to replicaConnections");
                        }
                        writer.flush();
                        continue;
                    } else if (command.equalsIgnoreCase("SET") && Main.master == null) { // Master only
                        reader.readLine(); // e.g., $3
                        String key = reader.readLine(); // e.g., foo
                        reader.readLine(); // e.g., $3
                        String value = reader.readLine(); // e.g., 123
                        map.put(key, value);
                        expiryMap.remove(key);
                        StringBuilder sb = new StringBuilder();
                        sb.append("*3\r\n$3\r\nSET\r\n$").append(key.length()).append("\r\n").append(key).append("\r\n$").append(value.length()).append("\r\n").append(value).append("\r\n");
                        writer.write("+OK\r\n".getBytes());
                        if (reader.ready()) {
                            reader.mark(1000);
                            String maybeDollar = reader.readLine();
                            if ("$2".equalsIgnoreCase(maybeDollar)) {
                                String keyword = reader.readLine();
                                if ("px".equalsIgnoreCase(keyword)) {
                                    reader.readLine();
                                    String millisStr = reader.readLine();
                                    try {
                                        long expireAt = System.currentTimeMillis() + Long.parseLong(millisStr);
                                        expiryMap.put(key, expireAt);
                                    } catch (NumberFormatException ignored) {
                                    }
                                }
                            }
                        }
                        propagateToReplicas(new String[]{"SET", key, value});
                        writer.flush();
                        continue;
                    }
                }

                if (line.trim().equalsIgnoreCase("PING")) {
                    writer.write("+PONG\r\n".getBytes());
                    writer.flush();
                    continue;
                }

                if (line.trim().equalsIgnoreCase("ECHO")) {
                    reader.readLine();
                    String value = reader.readLine();
                    String response = "$" + value.length() + "\r\n" + value + "\r\n";
                    writer.write(response.getBytes());
                    writer.flush();
                    continue;
                }

                if (line.trim().equalsIgnoreCase("CONFIG")) {
                    reader.readLine();
                    reader.readLine();
                    reader.readLine();
                    String param = reader.readLine();
                    String value = "";
                    if ("dir".equalsIgnoreCase(param)) value = Main.dir;
                    if ("dbfilename".equalsIgnoreCase(param)) value = Main.dbfilename;
                    String response = "*2\r\n" +
                            "$" + param.length() + "\r\n" + param + "\r\n" +
                            "$" + value.length() + "\r\n" + value + "\r\n";
                    writer.write(response.getBytes());
                    writer.flush();
                    continue;
                }

                if (line.trim().equalsIgnoreCase("GET")) {
                    reader.readLine();
                    String key = reader.readLine();
                    Long expireTime = expiryMap.get(key);
                    if (expireTime != null && System.currentTimeMillis() > expireTime) {
                        map.remove(key);
                        expiryMap.remove(key);
                        writer.write("$-1\r\n".getBytes());
                        writer.flush();
                        continue;
                    }
                    String value = map.get(key);
                    if (value != null) {
                        String resp = "$" + value.length() + "\r\n" + value + "\r\n";
                        writer.write(resp.getBytes());
                    } else {
                        writer.write("$-1\r\n".getBytes());
                    }
                    writer.flush();
                    continue;
                }

                if (line.trim().equalsIgnoreCase("KEYS")) {
                    reader.readLine();
                    String pattern = reader.readLine();
                    if ("*".equals(pattern)) {
                        long now = System.currentTimeMillis();
                        StringBuilder response = new StringBuilder();
                        int count = 0;
                        for (String key : map.keySet()) {
                            Long exp = expiryMap.get(key);
                            if (exp != null && now > exp) {
                                map.remove(key);
                                expiryMap.remove(key);
                                continue;
                            }
                            count++;
                        }
                        response.append("*").append(count).append("\r\n");
                        for (String key : map.keySet()) {
                            Long exp = expiryMap.get(key);
                            if (exp != null && now > exp) continue;
                            response.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                        }
                        writer.write(response.toString().getBytes());
                    } else {
                        writer.write("*0\r\n".getBytes());
                    }
                    writer.flush();
                    continue;
                }

                if (line.trim().equalsIgnoreCase("INFO")) {
                    reader.readLine();
                    String section = reader.readLine();
                    if ("replication".equalsIgnoreCase(section)) {
                        StringBuilder info;
                        if (Main.master != null) {
                            info = new StringBuilder("role:slave");
                        } else {
                            info = new StringBuilder("role:master");
                        }
                        info.append("\r\n");
                        info.append("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
                        info.append("\r\n");
                        info.append("master_repl_offset:").append(Main.masterOffset).append("\r\n");
                        String response = "$" + info.length() + "\r\n" + info + "\r\n";
                        writer.write(response.getBytes());
                    } else {
                        writer.write("$-1\r\n".getBytes());
                    }
                    writer.flush();
                    continue;
                }
            }
        } catch (IOException e) {
            System.err.println("Client error: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Socket close failed: " + e.getMessage());
            }
        }
    }
}