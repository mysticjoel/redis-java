import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class Handshake {
    private final String masterHost;
    private final int masterPort;
    public Handshake(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }
    public void start(){
        try(Socket socket = new Socket(masterHost, masterPort)){
            OutputStream out = socket.getOutputStream();

            String command = "*1\r\n$4\r\nPING\r\n";
            out.write(command.getBytes());
            out.flush();
            System.out.println("Sent PING to master at " + masterHost + ":" + masterPort);

        }catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
