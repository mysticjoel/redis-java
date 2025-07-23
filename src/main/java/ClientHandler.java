import java.io.*;
import java.net.Socket;
import java.util.Base64;
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
        for (OutputStream out : Main.replicaOutputStreams) {
            try {
                out.write(payload);
                out.flush();
            } catch (IOException e) {
                // Ignore or log
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
            while ((line = reader.readLine()) != null) {
                if (line.trim().equalsIgnoreCase("PING")) {
                    writer.write("+PONG\r\n".getBytes());
                    continue;
                }

                if (line.trim().equalsIgnoreCase("ECHO")) {
                    reader.readLine(); // skip $length
                    String value = reader.readLine();
                    String response = "$" + value.length() + "\r\n" + value + "\r\n";
                    writer.write(response.getBytes());
                    continue;
                }

                if (line.trim().equalsIgnoreCase("CONFIG")) {
                    reader.readLine(); // $3
                    reader.readLine(); // GET
                    reader.readLine(); // $length
                    String param = reader.readLine();

                    String value = "";
                    if ("dir".equalsIgnoreCase(param)) value = Main.dir;
                    if ("dbfilename".equalsIgnoreCase(param)) value = Main.dbfilename;

                    String response = "*2\r\n" +
                            "$" + param.length() + "\r\n" + param + "\r\n" +
                            "$" + value.length() + "\r\n" + value + "\r\n";
                    writer.write(response.getBytes());
                    continue;
                }

                if (line.trim().equalsIgnoreCase("SET")) {
                    reader.readLine(); // $<key_length>
                    String key = reader.readLine();
                    reader.readLine(); // $<value_length>
                    String value = reader.readLine();
                    map.put(key, value);
                    expiryMap.remove(key);
                    writer.write("+OK\r\n".getBytes());

                    // Handle PX option (expiration)
                    if (reader.ready()) {
                        reader.mark(1000); // mark before checking PX
                        String maybeDollar = reader.readLine();
                        if ("$2".equalsIgnoreCase(maybeDollar)) {
                            String keyword = reader.readLine(); // e.g., "px"
                            if ("px".equalsIgnoreCase(keyword)) {
                                reader.readLine(); // $<length of milliseconds>
                                String millisStr = reader.readLine();
                                try {
                                    long expireAt = System.currentTimeMillis() + Long.parseLong(millisStr);
                                    expiryMap.put(key, expireAt);
                                } catch (NumberFormatException ignored) {
                                }
                            } else {
                                reader.reset(); // rollback if not PX
                            }
                        } else {
                            reader.reset(); // rollback if not $2
                        }
                    }
                    propagateToReplicas(new String[]{"SET", key, value});
                    continue;
                }

                if (line.trim().equalsIgnoreCase("DEL")) {
                    reader.readLine();
                    String key = reader.readLine();
                    map.remove(key);
                    expiryMap.remove(key);
                    writer.write(":1\r\n".getBytes());
                    propagateToReplicas(new String[]{"DEL", key});
                    continue;
                }

                if (line.trim().equalsIgnoreCase("GET")) {
                    reader.readLine(); // $length
                    String key = reader.readLine();
                    Long expireTime = expiryMap.get(key);
                    if (expireTime != null && System.currentTimeMillis() > expireTime) {
                        map.remove(key);
                        expiryMap.remove(key);
                        writer.write("$-1\r\n".getBytes());
                        continue;
                    }

                    String value = map.get(key);
                    if (value != null) {
                        String resp = "$" + value.length() + "\r\n" + value + "\r\n";
                        writer.write(resp.getBytes());
                    } else {
                        writer.write("$-1\r\n".getBytes());
                    }
                    continue;
                }

                if (line.trim().equalsIgnoreCase("KEYS")) {
                    reader.readLine(); // $1
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
                        info.append("master_repl_offset:0");
                        info.append("\r\n");
                        String response = "$" + info.length() + "\r\n" + info + "\r\n";
                        writer.write(response.getBytes());
                    } else {
                        writer.write("$-1\r\n".getBytes());
                    }
                    continue;
                }

                if (line.trim().equalsIgnoreCase("REPLCONF")) {
                    writer.write("+OK\r\n".getBytes());
                    continue;
                }

                if (line.trim().equalsIgnoreCase("PSYNC")) {
                    writer.write("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".getBytes());
                    writer.write(("$" + emptyRDB.length + "\r\n").getBytes());
                    writer.write(emptyRDB);
                    Main.replicaOutputStreams.add(clientSocket.getOutputStream());
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