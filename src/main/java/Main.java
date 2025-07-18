import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {

    static String dir = null;
    static String dbfilename = null;

    public static void main(String[] args) {
        // Parse command-line arguments
        for (int i = 0; i < args.length; i++) {
            if ("--dir".equals(args[i]) && i + 1 < args.length) {
                dir = args[i + 1];
                i++;
            } else if ("--dbfilename".equals(args[i]) && i + 1 < args.length) {
                dbfilename = args[i + 1];
                i++;
            }
        }

        // Load RDB file
        if (dir != null && dbfilename != null) {
            File rdbFile = new File(dir, dbfilename);
            if (rdbFile.exists()) {
                try {
                    RDBParser.load(rdbFile);
                } catch (IOException e) {
                    System.err.println("Error loading RDB file: " + e.getMessage());
                }
            }
        }

        // Start Redis-like server
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Server listening on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }
}
