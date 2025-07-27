import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicationManager {
    private final List<ReplicaConnection> replicas = new ArrayList<>();
    private long masterOffset = 0;

    public void handlePsync(Socket socket, InputStream input, OutputStream output) throws IOException {
        String replicationId = "0123456789abcdef0123456789abcdef01234567";
        output.write(("+FULLRESYNC " + replicationId + " 0\r\n").getBytes());
        output.flush();

        String base64RDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        byte[] rdbBytes = Base64.getDecoder().decode(base64RDB);
        output.write(("$" + rdbBytes.length + "\r\n").getBytes());
        output.write(rdbBytes);
        output.flush();

        replicas.add(new ReplicaConnection(socket, input, output));
    }

    public void propagateCommand(String command, int bytesConsumed) throws IOException {
        masterOffset += bytesConsumed;
        for (ReplicaConnection replica : replicas) {
            try {
                replica.getOutputStream().write(command.getBytes());
                replica.getOutputStream().flush();
                replica.setOffset(replica.getOffset() + bytesConsumed);
            } catch (IOException e) {
                // Skip failed replica
            }
        }
    }

    public int waitForAcks(int requiredAcks, int timeoutMs, long masterOffset) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        List<ReplicaConnection> acked = new ArrayList<>();

        for (ReplicaConnection replica : replicas) {
            try {
                Writer writer = new OutputStreamWriter(replica.getOutputStream(), "UTF-8");
                writer.write("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
                writer.flush();
            } catch (IOException e) {
                // Skip failed write
            }
        }

        while (System.currentTimeMillis() < deadline) {
            for (ReplicaConnection replica : replicas) {
                if (acked.contains(replica)) continue;
                if (replica.getOffset() >= masterOffset && replica.isAcked()) {
                    acked.add(replica);
                }
            }
            if (acked.size() >= requiredAcks) break;
            try {
                Thread.sleep(5);
            } catch (InterruptedException ignored) {}
        }

        return masterOffset == 0 ? replicas.size() : acked.size();
    }

    public void updateReplicaOffset(Socket socket, long offset) {
        for (ReplicaConnection replica : replicas) {
            if (replica.getSocket().equals(socket)) {
                replica.setOffset(offset);
                replica.setAcked(true);
                break;
            }
        }
    }

    public long getMasterOffset() {
        return masterOffset;
    }
}