import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    public static String dir = "";
    public static String dbfilename = "";
    public static String master = null;
    public static long masterOffset = 0;
    public static final Map<Integer, ReplicaConnection> replicaConnections = Collections.synchronizedMap(new HashMap<>());

    public static class ReplicaConnection {
        public final Socket socket;
        public final OutputStream outputStream;
        public final BufferedReader inputStream;
        public final int port;

        public ReplicaConnection(Socket socket, int port) throws IOException {
            this.socket = socket;
            this.port = port;
            this.outputStream = socket.getOutputStream();
            this.inputStream = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        }
    }

    public static void main(String[] args) throws IOException {
        int port = 6379;
        String masterHost = null;
        int masterPort = 0;

        // Parse arguments
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port")) {
                port = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--replicaof")) {
                masterHost = args[++i];
                masterPort = Integer.parseInt(args[++i]);
                master = masterHost + ":" + masterPort;
            } else if (args[i].equals("--dir")) {
                dir = args[++i];
            } else if (args[i].equals("--dbfilename")) {
                dbfilename = args[++i];
            }
        }

        // Start server
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Server started on port " + port);
        ExecutorService executor = Executors.newCachedThreadPool();

        // Start handshake for replica
        if (master != null) {
            System.out.println("Starting handshake for replica on port " + port + " to master " + master);
            Handshake handshake = new Handshake(masterHost, masterPort, port);
            new Thread(handshake).start();
        }

        // Accept client connections
        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("Accepted client connection on port " + port);
            ClientHandler clientHandler = new ClientHandler(clientSocket);
            executor.submit(clientHandler);
        }
    }
}