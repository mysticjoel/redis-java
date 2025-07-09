import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.*;

public class Main {

    static String dir = null;
    static String dbfilename = null;

    public static void main(String[] args) {
        // Parse command line args for --dir and --dbfilename
        for (int i = 0; i < args.length; i++) {
            if ("--dir".equals(args[i]) && i + 1 < args.length) {
                dir = args[i + 1];
                i++;
            } else if ("--dbfilename".equals(args[i]) && i + 1 < args.length) {
                dbfilename = args[i + 1];
                i++;
            }
        }
        System.out.println(dir);
        System.out.println(dbfilename);

        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Server listening on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}



class ClientHandler implements Runnable {
    private Socket clientSocket;
    //private final HashMap<String, String> map = new HashMap<>();
    private static final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> expiryMap = new ConcurrentHashMap<>();

    //private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
    }
    public void run() {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                OutputStream writer = clientSocket.getOutputStream()
        ) {
            String line;
            //HashMap<String,String> map = new HashMap<>();
            //ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            //long expirationTime = 0;
            while ((line = reader.readLine()) != null) {
                if (line.trim().equalsIgnoreCase("PING")) {
                    writer.write("+PONG\r\n".getBytes());
                    continue;
                }
                if (line.trim().equalsIgnoreCase("ECHO")) {
                    // Read next 2 lines for bulk string:
                    reader.readLine(); // skip $length
                    String value = reader.readLine(); // get the actual value
                    String response = "$" + value.length() + "\r\n" + value + "\r\n";
                    writer.write(response.getBytes());
                }

                if (line.trim().equalsIgnoreCase("CONFIG")) {
                    // Expect next line: GET
                    reader.readLine();
                    String subCommand = reader.readLine();
                    if (subCommand != null && subCommand.trim().equalsIgnoreCase("GET")) {
                        reader.readLine(); // skip $length line for parameter name
                        String param = reader.readLine(); // parameter name: e.g. "dir" or "dbfilename"

                        String value = null;
                        if ("dir".equalsIgnoreCase(param)) {
                            value = Main.dir != null ? Main.dir : "";
                        } else if ("dbfilename".equalsIgnoreCase(param)) {
                            value = Main.dbfilename != null ? Main.dbfilename : "";
                        } else {
                            value = ""; // Unknown parameter - empty string response
                        }

                        // Prepare RESP array response of 2 bulk strings: param and value
                        String response =
                                "*2\r\n" +
                                        "$" + param.length() + "\r\n" +
                                        param + "\r\n" +
                                        "$" + value.length() + "\r\n" +
                                        value + "\r\n";

                        writer.write(response.getBytes());
                        writer.flush();
                        //break;
                        continue;
                    }
                }


                if(line.trim().equalsIgnoreCase("SET")){
                    System.out.println(line);
                    reader.readLine();
                    String key = reader.readLine();
                    reader.readLine();
                    String value = reader.readLine();
                    map.put(key,value);
                    expiryMap.remove(key);
                    System.out.println(map);
                    System.out.println(expiryMap);
                    writer.write(("+OK\r\n".getBytes()));
                    reader.mark(1000);
                    String possibleDollarLine = reader.readLine(); // e.g., "$2"
                    if (possibleDollarLine != null && possibleDollarLine.trim().equalsIgnoreCase("$2")) {
                        String pxKeyword = reader.readLine(); // should be "px"
                        if (pxKeyword != null && pxKeyword.trim().equalsIgnoreCase("px")) {
                            reader.readLine(); // skip $length of px value
                            String timeStr = reader.readLine();
                            try {
                                int time = Integer.parseInt(timeStr.trim());
                                long expiryTime = System.currentTimeMillis() + time;
                                expiryMap.put(key, expiryTime);
                            } catch (NumberFormatException e) {
                                System.out.println("Invalid PX value: " + timeStr);
                            }
                        } else {
                            reader.reset(); // Not PX, rewind
                        }
                    } else {
                        reader.reset(); // No PX at all, rewind
                    }

                    continue;
                }
                if(line.trim().equalsIgnoreCase("GET")){
                    System.out.println(line);
                    reader.readLine();
                    String response = reader.readLine();
                    Long expiry = expiryMap.get(response);
                    if(expiry != null && System.currentTimeMillis() > expiry){
                        map.remove(response);
                        expiryMap.remove(response);
                        writer.write(("$-1\r\n".getBytes()));
                        continue;
                    }

                    if(map.get(response)!= null){
                        String value = map.get(response);
                        System.out.println(value);
                        String real = "$" + value.length() + "\r\n" + value + "\r\n";
                        writer.write(real.getBytes());
                    } else{
                        writer.write("$-1\r\n".getBytes());
                    }

                    //continue;
                }
            }
                // Optionally, handle other Redis-like commands here
            }catch (IOException e) {
            System.out.println("IOException in client handler: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("Failed to close client socket: " + e.getMessage());
            }
        }
    }
}
