import java.io.*;
import java.net.Socket;

public class ReplicaConnection {
    private final Socket socket;
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private long offset;
    private boolean acked;

    public ReplicaConnection(Socket socket, InputStream inputStream, OutputStream outputStream) {
        this.socket = socket;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.offset = 0;
        this.acked = false;
    }

    public ReplicaConnection(Socket socket) throws IOException {
        this(socket, socket.getInputStream(), socket.getOutputStream());
    }

    public Socket getSocket() {
        return socket;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public boolean isAcked() {
        return acked;
    }

    public void setAcked(boolean acked) {
        this.acked = acked;
    }
}