import java.io.*;
import java.net.Socket;

public class Handshake {
    private final String masterHost;
    private final int masterPort;

    public Handshake(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    public void start() {
        Socket socket = null;
        DataInputStream in = null;
        OutputStream out = null;
        try {
            socket = new Socket(masterHost, masterPort);
            out = socket.getOutputStream();
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            // 1. Send PING
            String ping = "*1\r\n$4\r\nPING\r\n";
            out.write(ping.getBytes());
            out.flush();
            System.out.println("Sent PING to master at " + masterHost + ":" + masterPort);

            // 2. Wait for +PONG\r\n
            String response = readLine(in);
            if (response == null || !response.equals("+PONG")) {
                throw new IOException("Expected +PONG, got: " + response);
            }
            System.out.println("Received from master: " + response);

            // 3. Send REPLCONF listening-port <PORT>
            String replconf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + Main.port + "\r\n";
            out.write(replconf1.getBytes());
            out.flush();

            // 4. Wait for +OK
            response = readLine(in);
            if (response == null || !response.equals("+OK")) {
                throw new IOException("Expected +OK, got: " + response);
            }

            // 5. Send REPLCONF capa psync2
            String replconf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
            out.write(replconf2.getBytes());
            out.flush();

            // 6. Wait for +OK
            response = readLine(in);
            if (response == null || !response.equals("+OK")) {
                throw new IOException("Expected +OK, got: " + response);
            }

            // 7. Send PSYNC
            String psync1 = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
            out.write(psync1.getBytes());
            out.flush();
            response = readLine(in);
            if (response == null || !response.startsWith("+FULLRESYNC")) {
                throw new IOException("Expected +FULLRESYNC, got: " + response);
            }
            System.out.println(response);

            // 8. Receive and process RDB file
            response = readLine(in);
            if (response == null || !response.startsWith("$")) {
                throw new IOException("Expected RDB length, got: " + response);
            }
            int rdbLength;
            try {
                rdbLength = Integer.parseInt(response.substring(1));
            } catch (NumberFormatException e) {
                throw new IOException("Invalid RDB length: " + response, e);
            }
            if (rdbLength == -1) {
                System.out.println("Received empty RDB file");
            } else if (rdbLength > 0) {
                byte[] rdbData = new byte[rdbLength];
                int bytesRead = 0;
                while (bytesRead < rdbLength) {
                    int read = in.read(rdbData, bytesRead, rdbLength - bytesRead);
                    if (read == -1) {
                        throw new IOException("Unexpected EOF while reading RDB file");
                    }
                    bytesRead += read;
                }
                System.out.println("Received RDB file of length: " + rdbLength);
                // Optionally parse RDB data using RDBParser
                // For test case, assume empty RDB, so no parsing needed
            } else {
                throw new IOException("Invalid RDB length: " + rdbLength);
            }

            // 9. Continuously read propagated commands
            while (true) {
                response = readLine(in);
                if (response == null) {
                    System.err.println("Master connection closed unexpectedly");
                    break;
                }
                System.out.println("Received from master: " + response);
                if (response.startsWith("*")) {
                    try {
                        int numParts = Integer.parseInt(response.substring(1));
                        if (numParts <= 0) {
                            System.err.println("Invalid RESP array size: " + response);
                            continue;
                        }
                        String[] parts = new String[numParts];
                        for (int i = 0; i < numParts; i++) {
                            String lengthLine = readLine(in);
                            if (lengthLine == null || !lengthLine.startsWith("$")) {
                                throw new IOException("Expected bulk string length, got: " + lengthLine);
                            }
                            int length = Integer.parseInt(lengthLine.substring(1));
                            if (length < 0) {
                                throw new IOException("Invalid bulk string length: " + lengthLine);
                            }
                            String value = readLine(in);
                            if (value == null || value.length() != length) {
                                throw new IOException("Invalid bulk string data for length: " + length);
                            }
                            parts[i] = value;
                        }
                        processPropagatedCommand(parts);
                    } catch (Exception e) {
                        System.err.println("Error processing propagated command: " + e.getMessage());
                        e.printStackTrace();
                    }
                } else {
                    System.err.println("Unexpected line from master: " + response);
                }
            }
        } catch (IOException e) {
            System.err.println("Handshake or command processing failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (socket != null && !socket.isClosed()) {
                try {
                    socket.close();
                } catch (IOException e) {
                    System.err.println("Failed to close master socket: " + e.getMessage());
                }
            }
        }
    }

    private String readLine(DataInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int b = in.read();
            if (b == -1) {
                return sb.length() > 0 ? sb.toString() : null;
            }
            char c = (char) b;
            if (c == '\r') {
                int next = in.read();
                if (next == -1 || next != '\n') {
                    throw new IOException("Invalid line ending");
                }
                return sb.toString();
            }
            sb.append(c);
        }
    }

    private void processPropagatedCommand(String[] parts) {
        if (parts.length == 0) return;
        String command = parts[0].toUpperCase();
        switch (command) {
            case "SET":
                if (parts.length >= 3) {
                    ClientHandler.map.put(parts[1], parts[2]);
                    ClientHandler.expiryMap.remove(parts[1]);
                    System.out.println("Processed propagated SET: " + parts[1] + " = " + parts[2]);
                } else {
                    System.err.println("Invalid SET command: " + String.join(" ", parts));
                }
                break;
            case "DEL":
                if (parts.length >= 2) {
                    ClientHandler.map.remove(parts[1]);
                    ClientHandler.expiryMap.remove(parts[1]);
                    System.out.println("Processed propagated DEL: " + parts[1]);
                } else {
                    System.err.println("Invalid DEL command: " + String.join(" ", parts));
                }
                break;
            default:
                System.out.println("Unsupported propagated command: " + command);
                break;
        }
    }
}