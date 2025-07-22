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
        try (Socket socket = new Socket(masterHost, masterPort)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            // 1. Send PING
            String ping = "*1\r\n$4\r\nPING\r\n";
            out.write(ping.getBytes());
            out.flush();
            System.out.println("Sent PING to master at " + masterHost + ":" + masterPort);

            // 2. Wait for +PONG\r\n
            String response = reader.readLine();  // should be "+PONG"
            System.out.println("Received from master: " + response);

            // 3. Send REPLCONF listening-port <PORT>
            String replconf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$\r\n" + Main.port + "\r\n";
            out.write(replconf1.getBytes());
            out.flush();

            // 4. Wait for +OK
            reader.readLine();

            // 5. Send REPLCONF capa psync2
            String replconf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
            out.write(replconf2.getBytes());
            out.flush();

            // 6. Wait for +OK
            reader.readLine();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
