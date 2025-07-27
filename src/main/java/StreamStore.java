import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStore {
    private final Map<String, List<StreamEntry>> streams = new ConcurrentHashMap<>();

    public static class StreamEntry {
        public final String id;
        public final Map<String, String> fields;

        public StreamEntry(String id, Map<String, String> fields) {
            this.id = id;
            this.fields = new HashMap<>(fields);
        }
    }

    public String add(String key, String entryId, Map<String, String> fields) {
        List<StreamEntry> stream = streams.computeIfAbsent(key, k -> new ArrayList<>());
        if (entryId.equals("0-0")) {
            throw new IllegalArgumentException("The ID specified in XADD must be greater than 0-0");
        }

        String[] parts = entryId.split("-");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid ID format");
        }
        long ms;
        try {
            ms = Long.parseLong(parts[0]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid millisecond time in ID");
        }
        long seq;
        boolean autoGenerateSeq = parts[1].equals("*");

        if (autoGenerateSeq) {
            if (stream.isEmpty()) {
                seq = ms == 0 ? 1 : 0; // Use 1 for ms=0, 0 otherwise
            } else {
                StreamEntry lastEntry = stream.get(stream.size() - 1);
                String[] lastParts = lastEntry.id.split("-");
                long lastMs = Long.parseLong(lastParts[0]);
                long lastSeq = Long.parseLong(lastParts[1]);
                if (ms < lastMs) {
                    throw new IllegalArgumentException("The ID specified in XADD is equal or smaller than the target stream top item");
                }
                seq = (ms == lastMs) ? lastSeq + 1 : 0;
            }
        } else {
            try {
                seq = Long.parseLong(parts[1]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid sequence number in ID");
            }
            if (!stream.isEmpty()) {
                StreamEntry lastEntry = stream.get(stream.size() - 1);
                String[] lastParts = lastEntry.id.split("-");
                long lastMs = Long.parseLong(lastParts[0]);
                long lastSeq = Long.parseLong(lastParts[1]);
                if (ms < lastMs || (ms == lastMs && seq <= lastSeq)) {
                    throw new IllegalArgumentException("The ID specified in XADD is equal or smaller than the target stream top item");
                }
            }
        }

        String newId = ms + "-" + seq;
        stream.add(new StreamEntry(newId, fields));
        return newId;
    }

    public List<StreamEntry> range(String key, String start, String end) {
        List<StreamEntry> stream = streams.getOrDefault(key, new ArrayList<>());
        List<StreamEntry> result = new ArrayList<>();
        boolean include = false;
        for (StreamEntry entry : stream) {
            if (start.equals("-") || entry.id.compareTo(start) >= 0) {
                include = true;
            }
            if (include) {
                if (end.equals("+") || entry.id.compareTo(end) <= 0) {
                    result.add(entry);
                } else {
                    break;
                }
            }
        }
        return result;
    }

    public Map<String, List<StreamEntry>> read(List<String> keys, List<String> startIds, long blockMs) {
        Map<String, List<StreamEntry>> result = new HashMap<>();
        long deadline = blockMs > 0 ? System.currentTimeMillis() + blockMs : 0;

        while (true) {
            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                String startId = startIds.get(i);
                List<StreamEntry> stream = streams.getOrDefault(key, new ArrayList<>());
                List<StreamEntry> entries = new ArrayList<>();
                for (StreamEntry entry : stream) {
                    if (entry.id.compareTo(startId) > 0) {
                        entries.add(entry);
                    }
                }
                if (!entries.isEmpty()) {
                    result.put(key, entries);
                }
            }
            if (!result.isEmpty() || blockMs == 0 || System.currentTimeMillis() >= deadline) {
                break;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {}
        }
        return result;
    }

    public String type(String key) {
        return streams.containsKey(key) ? "stream" : "none";
    }
}