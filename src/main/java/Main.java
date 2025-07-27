import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.*;
import java.util.*;

public class Main {
    private static final int DEFAULT_PORT = 6379;
    private final Map<String, String> config = new HashMap<>();
    private final KeyValueStore kvStore = new KeyValueStore();
    private final StreamStore streamStore = new StreamStore();
    private final ListStore listStore = new ListStore();
    private final ReplicationManager replicationManager = new ReplicationManager();

    public static void main(String[] args) {
        try {
            new Main().start(args);
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }

    public void start(String[] args) throws IOException {
        parseArguments(args);
        loadRDBFile();

        int port = config.containsKey("port") ? Integer.parseInt(config.get("port")) : DEFAULT_PORT;
        if (config.containsKey("replicaof")) {
            new Thread(() -> new ReplicaClient(config, kvStore, replicationManager).connect()).start();
        }

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected");
                new Thread(() -> new ClientHandler(clientSocket, kvStore, streamStore,
                        listStore, replicationManager, config).handle()).start();
            }
        }
    }

    private void parseArguments(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--dir":
                case "--dbfilename":
                case "--port":
                case "--replicaof":
                    if (i + 1 < args.length) {
                        config.put(args[i].substring(2), args[i + 1]);
                        i++;
                    }
                    break;
            }
        }
    }

    private void loadRDBFile() {
        if (!config.containsKey("dir") || !config.containsKey("dbfilename")) return;

        Path path = Paths.get(config.get("dir"), config.get("dbfilename"));
        try {
            byte[] bytes = Files.readAllBytes(path);
            int databaseSectionOffset = -1;
            for (int i = 0; i < bytes.length; i++) {
                if (bytes[i] == (byte) 0xfe) {
                    databaseSectionOffset = i;
                    break;
                }
            }

            for (int i = databaseSectionOffset + 4; i < bytes.length; i++) {
                long expiryTime = Long.MAX_VALUE;
                if (bytes[i] == (byte) 0xfc && i + 8 < bytes.length) {
                    byte[] expBytes = Arrays.copyOfRange(bytes, i + 1, i + 9);
                    ByteBuffer buffer = ByteBuffer.wrap(expBytes).order(ByteOrder.LITTLE_ENDIAN);
                    expiryTime = buffer.getLong();
                    i += 9;
                }

                if (bytes[i] == (byte) 0x00 && i + 1 < bytes.length) {
                    int keyLen = bytes[i + 1] & 0xFF;
                    if (keyLen <= 0) continue;
                    String key = new String(Arrays.copyOfRange(bytes, i + 2, i + 2 + keyLen));
                    i += 2 + keyLen;
                    if (i >= bytes.length) break;
                    int valueLen = bytes[i] & 0xFF;
                    if (valueLen <= 0) continue;
                    String value = new String(Arrays.copyOfRange(bytes, i + 1, i + 1 + valueLen));
                    i += valueLen;
                    kvStore.set(key, value, expiryTime);
                }
            }
        } catch (IOException e) {
            System.out.println("RDB file error: " + e.getMessage());
        }
    }
}