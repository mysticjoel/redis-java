import java.io.*;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    public static final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<String, Long> expiryMap = new ConcurrentHashMap<>();

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
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
                    String subCommand = reader.readLine(); // GET
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
                    reader.readLine(); // $length
                    String key = reader.readLine();
                    reader.readLine(); // $length
                    String value = reader.readLine();
                    map.put(key, value);
                    expiryMap.remove(key);
                    writer.write("+OK\r\n".getBytes());

                    // Optional PX expiration
                    reader.mark(1000);
                    String maybeDollar = reader.readLine();
                    if ("$2".equalsIgnoreCase(maybeDollar)) {
                        String keyword = reader.readLine(); // px
                        if ("px".equalsIgnoreCase(keyword)) {
                            reader.readLine(); // $length
                            String millisStr = reader.readLine();
                            try {
                                long expireAt = System.currentTimeMillis() + Long.parseLong(millisStr);
                                expiryMap.put(key, expireAt);
                            } catch (NumberFormatException ignored) {
                            }
                        } else {
                            reader.reset();
                        }
                    } else {
                        reader.reset();
                    }
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
