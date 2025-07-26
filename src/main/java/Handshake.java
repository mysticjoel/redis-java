import java.io.*;
import java.net.Socket;

public class Handshake implements Runnable {
    private final String masterHost;
    private final int masterPort;
    private final int replicaPort;
    private long replicaOffset = 0; // Per-replica offset

    public Handshake(String masterHost, int masterPort, int replicaPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.replicaPort = replicaPort;
    }

    @Override
    public void run() {
        Socket masterSocket = null;
        try {
            masterSocket = new Socket(masterHost, masterPort);
            BufferedReader reader = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
            OutputStream writer = masterSocket.getOutputStream();

            // PING
            writer.write("*1\r\n$4\r\nPING\r\n".getBytes());
            writer.flush();
            String pong = reader.readLine();
            System.out.println("Handshake PING response for replica@" + replicaPort + ": " + pong);

            // REPLCONF listening-port
            writer.write(("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + replicaPort + "\r\n").getBytes());
            writer.flush();
            String ok1 = reader.readLine();
            System.out.println("Handshake REPLCONF listening-port response for replica@" + replicaPort + ": " + ok1);

            // REPLCONF capa psync2
            writer.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".getBytes());
            writer.flush();
            String ok2 = reader.readLine();
            System.out.println("Handshake REPLCONF capa response for replica@" + replicaPort + ": " + ok2);

            // PSYNC ? -1
            writer.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes());
            writer.flush();
            String fullresync = reader.readLine();
            System.out.println("Handshake PSYNC response for replica@" + replicaPort + ": " + fullresync);
            String rdbLen = reader.readLine();
            System.out.println("Handshake RDB length for replica@" + replicaPort + ": " + rdbLen);
            int len = Integer.parseInt(rdbLen.substring(1));
            char[] rdb = new char[len];
            reader.read(rdb, 0, len);
            System.out.println("Handshake RDB received for replica@" + replicaPort + ": " + len + " bytes");

            // Process master commands (SET, DEL, REPLCONF GETACK)
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("Replica@" + replicaPort + " received line from master: " + line);
                if (line.trim().equalsIgnoreCase("*3")) {
                    String commandLength = reader.readLine();
                    String command = reader.readLine();
                    System.out.println("Replica@" + replicaPort + " received *3 command: " + command);
                    if (command.equalsIgnoreCase("SET")) {
                        reader.readLine(); // e.g., $3
                        String key = reader.readLine(); // e.g., foo
                        reader.readLine(); // e.g., $3
                        String value = reader.readLine(); // e.g., 123
                        ClientHandler.map.put(key, value);
                        ClientHandler.expiryMap.remove(key);
                        StringBuilder sb = new StringBuilder();
                        sb.append("*3\r\n$3\r\nSET\r\n$").append(key.length()).append("\r\n").append(key).append("\r\n$").append(value.length()).append("\r\n").append(value).append("\r\n");
                        synchronized (this) {
                            replicaOffset += sb.toString().length();
                        }
                        System.out.println("Replica@" + replicaPort + " processed SET: key=" + key + ", value=" + value + ", replicaOffset=" + replicaOffset);
                    } else if (command.equalsIgnoreCase("REPLCONF")) {
                        reader.readLine(); // e.g., $6
                        String subcommand = reader.readLine(); // e.g., GETACK
                        reader.readLine(); // e.g., $1
                        String arg = reader.readLine(); // e.g., *
                        System.out.println("Replica@" + replicaPort + " received REPLCONF subcommand: " + subcommand + ", arg: " + arg);
                        if (subcommand.equalsIgnoreCase("GETACK")) {
                            String offsetStr = String.valueOf(replicaOffset);
                            String response = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + offsetStr.length() + "\r\n" + offsetStr + "\r\n";
                            System.out.println("Replica@" + replicaPort + " sending ACK with offset: " + offsetStr);
                            writer.write(response.getBytes());
                            writer.flush();
                        }
                    }
                } else if (line.trim().equalsIgnoreCase("*2")) {
                    String commandLength = reader.readLine();
                    String command = reader.readLine();
                    System.out.println("Replica@" + replicaPort + " received *2 command: " + command);
                    if (command.equalsIgnoreCase("DEL")) {
                        reader.readLine(); // e.g., $<len>
                        String key = reader.readLine(); // e.g., key
                        ClientHandler.map.remove(key);
                        ClientHandler.expiryMap.remove(key);
                        StringBuilder sb = new StringBuilder();
                        sb.append("*2\r\n$3\r\nDEL\r\n$").append(key.length()).append("\r\n").append(key).append("\r\n");
                        synchronized (this) {
                            replicaOffset += sb.toString().length();
                        }
                        System.out.println("Replica@" + replicaPort + " processed DEL: key=" + key + ", replicaOffset=" + replicaOffset);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Handshake error for replica@" + replicaPort + ": " + e.getMessage());
        } finally {
            if (masterSocket != null) {
                try {
                    masterSocket.close();
                } catch (IOException e) {
                    System.err.println("Error closing socket for replica@" + replicaPort + ": " + e.getMessage());
                }
            }
        }
    }
}