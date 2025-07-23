    import java.io.*;
    import java.net.ServerSocket;
    import java.net.Socket;
    import java.util.Set;
    import java.util.concurrent.ConcurrentHashMap;

    public class Main {

        static String dir = null;
        static String dbfilename = null;
        static int port = 6379;
        static String master = null;
        static String masterhost = null;
        static int masterport = 0;
        //static List<OutputStream> replicaOutputStreams = new ArrayList<>();
        public static final Set<OutputStream> replicaOutputStreams = ConcurrentHashMap.newKeySet();



        public static void main(String[] args) {
            // Parse command-line arguments
            for (int i = 0; i < args.length; i++) {
                if ("--dir".equals(args[i]) && i + 1 < args.length) {
                    dir = args[i + 1];
                    i++;
                } else if ("--dbfilename".equals(args[i]) && i + 1 < args.length) {
                    dbfilename = args[i + 1];
                    i++;
                } else if("--port".equals(args[i]) && i + 1 < args.length) {
                    port = Integer.parseInt(args[i + 1]);
                } else if ("--replicaof".equals(args[i]) && i + 1 < args.length) {
                    master = args[i + 1];
                    String[] parts = master.split(" ");
                    masterhost = parts[0];
                    masterport = Integer.parseInt(parts[1]);
                }
            }

            //Handshake
            if (masterhost != null && masterport != 0) {
                Handshake handshake = new Handshake(masterhost, masterport);
                handshake.start();
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
            //int port = 6379;
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
