import java.io.*;
import java.net.Socket;
import java.util.*;

public class ClientHandler {
    private final Socket socket;
    private final KeyValueStore kvStore;
    private final StreamStore streamStore;
    private final ListStore listStore;
    private final ReplicationManager replicationManager;
    private final Map<String, String> config;
    private boolean transactionStarted = false;
    private final List<List<String>> transactionCommands = new ArrayList<>();

    public ClientHandler(Socket socket, KeyValueStore kvStore, StreamStore streamStore,
                         ListStore listStore, ReplicationManager replicationManager, Map<String, String> config) {
        this.socket = socket;
        this.kvStore = kvStore;
        this.streamStore = streamStore;
        this.listStore = listStore;
        this.replicationManager = replicationManager;
        this.config = config;
    }

    public void handle() {
        try (InputStream input = socket.getInputStream();
             OutputStream output = socket.getOutputStream()) {
            while (true) {
                RESPParser.ParseResult result = RESPParser.parseRESP(input);
                List<String> command = result.command;
                if (command.isEmpty()) continue;

                String cmd = command.get(0).toUpperCase();
                if (transactionStarted && (cmd.equals("SET") || cmd.equals("INCR") || cmd.equals("GET"))) {
                    transactionCommands.add(command);
                    writeResponse(output, "+QUEUED\r\n");
                    continue;
                }

                switch (cmd) {
                    case "PING":
                        writeResponse(output, "+PONG\r\n");
                        break;
                    case "ECHO":
                        writeResponse(output, RESPParser.buildBulkString(command.get(1)));
                        break;
                    case "SET":
                        handleSet(command, result.bytesConsumed, output);
                        break;
                    case "GET":
                        writeResponse(output, handleGet(command));
                        break;
                    case "CONFIG":
                        handleConfig(command, output);
                        break;
                    case "REPLCONF":
                        handleReplconf(command, output);
                        break;
                    case "PSYNC":
                        replicationManager.handlePsync(socket, input, output);
                        break;
                    case "KEYS":
                        handleKeys(command, output);
                        break;
                    case "INFO":
                        handleInfo(command, output);
                        break;
                    case "TYPE":
                        handleType(command, output);
                        break;
                    case "XADD":
                        handleXadd(command, output);
                        break;
                    case "XRANGE":
                        handleXrange(command, output);
                        break;
                    case "XREAD":
                        handleXread(command, output);
                        break;
                    case "INCR":
                        handleIncr(command, output);
                        break;
                    case "MULTI":
                        transactionStarted = true;
                        writeResponse(output, "+OK\r\n");
                        break;
                    case "EXEC":
                        handleExec(output);
                        break;
                    case "DISCARD":
                        handleDiscard(output);
                        break;
                    case "RPUSH":
                        handleRpush(command, output);
                        break;
                    case "LRANGE":
                        handleLrange(command, output);
                        break;
                    case "LPUSH":
                        handleLpush(command, output);
                        break;
                    case "LLEN":
                        writeResponse(output, ":" + listStore.llen(command.get(1)) + "\r\n");
                        break;
                    case "LPOP":
                        handleLpop(command, output);
                        break;
                    case "BLPOP":
                        handleBlpop(command, output);
                        break;
                    case "WAIT":
                        handleWait(command, output);
                        break;
                    default:
                        writeResponse(output, "-ERR unknown command\r\n");
                }
            }
        } catch (IOException e) {
            System.out.println("Client error: " + e.getMessage());
        }
    }

    private void writeResponse(OutputStream output, String response) throws IOException {
        output.write(response.getBytes("UTF-8"));
        output.flush();
    }

    private void handleSet(List<String> command, int bytesConsumed, OutputStream output) throws IOException {
        String key = command.get(1);
        String value = command.get(2);
        long expiryTime = Long.MAX_VALUE;
        if (command.size() >= 5 && command.get(3).equalsIgnoreCase("PX")) {
            try {
                expiryTime = System.currentTimeMillis() + Long.parseLong(command.get(4));
            } catch (NumberFormatException e) {
                writeResponse(output, "-ERR invalid PX value\r\n");
                return;
            }
        }
        kvStore.set(key, value, expiryTime);
        replicationManager.propagateCommand(RESPParser.buildArray("SET", key, value), bytesConsumed);
        writeResponse(output, "+OK\r\n");
    }

    private String handleGet(List<String> command) {
        String value = kvStore.get(command.get(1));
        return value != null ? RESPParser.buildBulkString(value) : "$-1\r\n";
    }

    private void handleConfig(List<String> command, OutputStream output) throws IOException {
        if (command.size() >= 3 && command.get(1).equalsIgnoreCase("GET")) {
            String value = config.get(command.get(2));
            writeResponse(output, value != null ?
                    RESPParser.buildArray(command.get(2), value) : "*0\r\n");
        } else {
            writeResponse(output, "-ERR wrong CONFIG usage\r\n");
        }
    }

    private void handleReplconf(List<String> command, OutputStream output) throws IOException {
        if (command.size() >= 3 && command.get(1).equalsIgnoreCase("ACK")) {
            replicationManager.updateReplicaOffset(socket, Long.parseLong(command.get(2)));
        } else if (!command.get(1).equalsIgnoreCase("GETACK")) {
            writeResponse(output, "+OK\r\n");
        }
    }

    private void handleKeys(List<String> command, OutputStream output) throws IOException {
        if (command.get(1).equals("*")) {
            StringBuilder response = new StringBuilder();
            response.append("*").append(kvStore.keys().size()).append("\r\n");
            for (String key : kvStore.keys()) {
                response.append(RESPParser.buildBulkString(key));
            }
            writeResponse(output, response.toString());
        }
    }

    private void handleInfo(List<String> command, OutputStream output) throws IOException {
        if (command.get(1).equalsIgnoreCase("replication")) {
            String info = config.containsKey("replicaof") ?
                    "role:slave" :
                    "role:master\nmaster_replid:0123456789abcdef0123456789abcdef01234567\nmaster_repl_offset:0";
            writeResponse(output, RESPParser.buildBulkString(info));
        }
    }

    private void handleType(List<String> command, OutputStream output) throws IOException {
        String key = command.get(1);
        String type = kvStore.type(key);
        if (type.equals("none")) type = streamStore.type(key);
        writeResponse(output, "+" + type + "\r\n");
    }

    private void handleXadd(List<String> command, OutputStream output) throws IOException {
        try {
            String streamKey = command.get(1);
            String entryId = command.get(2);
            Map<String, String> fields = new HashMap<>();
            for (int i = 3; i < command.size() - 1; i += 2) {
                fields.put(command.get(i), command.get(i + 1));
            }
            String resultId = streamStore.add(streamKey, entryId, fields);
            writeResponse(output, RESPParser.buildBulkString(resultId));
        } catch (IllegalArgumentException e) {
            writeResponse(output, "-ERR " + e.getMessage() + "\r\n");
        }
    }

    private void handleXrange(List<String> command, OutputStream output) throws IOException {
        List<StreamStore.StreamEntry> entries = streamStore.range(
                command.get(1), command.get(2), command.get(3));
        StringBuilder response = new StringBuilder();
        response.append("*").append(entries.size()).append("\r\n");
        for (StreamStore.StreamEntry entry : entries) {
            response.append("*2\r\n");
            response.append(RESPParser.buildBulkString(entry.id));
            response.append("*").append(entry.fields.size() * 2).append("\r\n");
            for (Map.Entry<String, String> field : entry.fields.entrySet()) {
                response.append(RESPParser.buildBulkString(field.getKey()));
                response.append(RESPParser.buildBulkString(field.getValue()));
            }
        }
        writeResponse(output, response.toString());
    }

    private void handleXread(List<String> command, OutputStream output) throws IOException {
        List<String> streamKeys = new ArrayList<>();
        List<String> startIds = new ArrayList<>();
        long blockMs = 0;

        if (command.get(1).equalsIgnoreCase("block")) {
            blockMs = Long.parseLong(command.get(2));
            int i = 3;
            while (i < command.size() && !command.get(i).equalsIgnoreCase("streams")) {
                i++;
            }
            i++; // Move past "streams"
            int keyCount = (command.size() - i) / 2;
            for (int j = i; j < i + keyCount; j++) {
                streamKeys.add(command.get(j));
            }
            for (int j = i + keyCount; j < command.size(); j++) {
                startIds.add(command.get(j));
            }
        } else {
            int i = 1;
            while (i < command.size() && !command.get(i).equalsIgnoreCase("streams")) {
                i++;
            }
            i++; // Move past "streams"
            int keyCount = (command.size() - i) / 2;
            for (int j = i; i < i + keyCount; j++) {
                streamKeys.add(command.get(j));
            }
            for (int j = i + keyCount; j < command.size(); j++) {
                startIds.add(command.get(j));
            }
        }

        Map<String, List<StreamStore.StreamEntry>> result = streamStore.read(streamKeys, startIds, blockMs);
        writeResponse(output, formatXreadResponse(result, streamKeys, blockMs));
    }

    private String formatXreadResponse(Map<String, List<StreamStore.StreamEntry>> result, List<String> keys, long blockMs) {
        int nonEmptyStreams = 0;
        for (String key : keys) {
            if (result.containsKey(key) && !result.get(key).isEmpty()) {
                nonEmptyStreams++;
            }
        }
        if (nonEmptyStreams == 0 && blockMs > 0) {
            return "$-1\r\n";
        }
        if (nonEmptyStreams == 0) {
            return "*0\r\n";
        }

        StringBuilder response = new StringBuilder();
        response.append("*").append(nonEmptyStreams).append("\r\n");
        for (String key : keys) {
            List<StreamStore.StreamEntry> entries = result.getOrDefault(key, new ArrayList<>());
            if (entries.isEmpty()) continue;
            response.append("*2\r\n");
            response.append(RESPParser.buildBulkString(key));
            response.append("*").append(entries.size()).append("\r\n");
            for (StreamStore.StreamEntry entry : entries) {
                response.append("*2\r\n");
                response.append(RESPParser.buildBulkString(entry.id));
                response.append("*").append(entry.fields.size() * 2).append("\r\n");
                for (Map.Entry<String, String> field : entry.fields.entrySet()) {
                    response.append(RESPParser.buildBulkString(field.getKey()));
                    response.append(RESPParser.buildBulkString(field.getValue()));
                }
            }
        }
        return response.toString();
    }

    private void handleIncr(List<String> command, OutputStream output) throws IOException {
        try {
            long newValue = kvStore.increment(command.get(1));
            writeResponse(output, ":" + newValue + "\r\n");
        } catch (IllegalArgumentException e) {
            writeResponse(output, "-ERR value is not an integer or out of range\r\n");
        }
    }

    private void handleExec(OutputStream output) throws IOException {
        if (!transactionStarted) {
            writeResponse(output, "-ERR EXEC without MULTI\r\n");
            return;
        }
        if (transactionCommands.isEmpty()) {
            writeResponse(output, "*0\r\n");
            transactionStarted = false;
            return;
        }

        StringBuilder response = new StringBuilder();
        response.append("*").append(transactionCommands.size()).append("\r\n");
        for (List<String> cmd : transactionCommands) {
            String command = cmd.get(0).toUpperCase();
            switch (command) {
                case "SET":
                    handleSet(cmd, 0, new ByteArrayOutputStream());
                    response.append("+OK\r\n");
                    break;
                case "INCR":
                    try {
                        long newValue = kvStore.increment(cmd.get(1));
                        response.append(":").append(newValue).append("\r\n");
                    } catch (IllegalArgumentException e) {
                        response.append("-ERR value is not an integer or out of range\r\n");
                    }
                    break;
                case "GET":
                    response.append(handleGet(cmd));
                    break;
                default:
                    response.append("-ERR Unsupported command in transaction\r\n");
            }
        }
        transactionStarted = false;
        transactionCommands.clear();
        writeResponse(output, response.toString());
    }

    private void handleDiscard(OutputStream output) throws IOException {
        if (!transactionStarted) {
            writeResponse(output, "-ERR DISCARD without MULTI\r\n");
        } else {
            transactionStarted = false;
            transactionCommands.clear();
            writeResponse(output, "+OK\r\n");
        }
    }

    private void handleRpush(List<String> command, OutputStream output) throws IOException {
        List<String> values = command.subList(2, command.size());
        int size = listStore.rpush(command.get(1), values);
        writeResponse(output, ":" + size + "\r\n");
    }

    private void handleLrange(List<String> command, OutputStream output) throws IOException {
        List<String> items = listStore.lrange(command.get(1),
                Integer.parseInt(command.get(2)), Integer.parseInt(command.get(3)));
        StringBuilder response = new StringBuilder();
        response.append("*").append(items.size()).append("\r\n");
        for (String item : items) {
            response.append(RESPParser.buildBulkString(item));
        }
        writeResponse(output, response.toString());
    }

    private void handleLpush(List<String> command, OutputStream output) throws IOException {
        List<String> values = command.subList(2, command.size());
        int size = listStore.lpush(command.get(1), values);
        writeResponse(output, ":" + size + "\r\n");
    }

    private void handleLpop(List<String> command, OutputStream output) throws IOException {
        int count = command.size() >= 3 ? Integer.parseInt(command.get(2)) : 1;
        List<String> items = listStore.lpop(command.get(1), count);
        if (items == null) {
            writeResponse(output, "$-1\r\n");
        } else if (count == 1) {
            writeResponse(output, RESPParser.buildBulkString(items.get(0)));
        } else {
            StringBuilder response = new StringBuilder();
            response.append("*").append(items.size()).append("\r\n");
            for (String item : items) {
                response.append(RESPParser.buildBulkString(item));
            }
            writeResponse(output, response.toString());
        }
    }

    private void handleBlpop(List<String> command, OutputStream output) throws IOException {
        double timeoutSeconds = Double.parseDouble(command.get(2));
        long timeoutMs = (long) (timeoutSeconds * 1000);
        List<String> result = listStore.blpop(command.get(1), timeoutMs);
        if (result == null) {
            writeResponse(output, "$-1\r\n");
        } else {
            StringBuilder response = new StringBuilder();
            response.append("*2\r\n");
            response.append(RESPParser.buildBulkString(command.get(1)));
            response.append(RESPParser.buildBulkString(result.get(0)));
            writeResponse(output, response.toString());
        }
    }

    private void handleWait(List<String> command, OutputStream output) throws IOException {
        int requiredAcks = Integer.parseInt(command.get(1));
        int timeoutMs = Integer.parseInt(command.get(2));
        long masterOffset = replicationManager.getMasterOffset();
        int acks = replicationManager.waitForAcks(requiredAcks, timeoutMs, masterOffset);
        writeResponse(output, ":" + acks + "\r\n");
    }
}