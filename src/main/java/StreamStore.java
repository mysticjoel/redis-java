import java.util.*;

public class StreamStore {
    public static class StreamEntry {
        String id;
        Map<String, String> fields;

        StreamEntry(String id, Map<String, String> fields) {
            this.id = id;
            this.fields = fields;
        }
    }

    private final Map<String, List<StreamEntry>> streams = new HashMap<>();

    public String add(String streamKey, String entryId, Map<String, String> fields) {
        String[] parts = entryId.split("-");
        long ts = parts.length == 2 ? Long.parseLong(parts[0]) : 0;
        long seq = parts.length == 2 && !parts[1].equals("*") ? Long.parseLong(parts[1]) : 0;

        if (entryId.equals("*")) {
            ts = System.currentTimeMillis();
            seq = getNextSequence(streamKey, ts);
            entryId = ts + "-" + seq;
        } else if (ts == 0 && parts[1].equals("*")) {
            seq = 1;
            entryId = "0-1";
        } else if (ts == 0 && seq == 0) {
            throw new IllegalArgumentException("ID must be greater than 0-0");
        }

        List<StreamEntry> entries = streams.computeIfAbsent(streamKey, k -> new ArrayList<>());
        if (!entries.isEmpty()) {
            String[] lastParts = entries.get(entries.size() - 1).id.split("-");
            long lastTs = Long.parseLong(lastParts[0]);
            long lastSeq = Long.parseLong(lastParts[1]);
            if (ts < lastTs || (ts == lastTs && seq <= lastSeq)) {
                throw new IllegalArgumentException("ID must be greater than last stream entry");
            }
        }

        entries.add(new StreamEntry(entryId, new HashMap<>(fields)));
        return entryId;
    }

    public List<StreamEntry> range(String streamKey, String startId, String endId) {
        List<StreamEntry> entries = streams.getOrDefault(streamKey, new ArrayList<>());
        if (entries.isEmpty()) return new ArrayList<>();

        if (endId.equals("+")) {
            endId = entries.get(entries.size() - 1).id;
        }
        if (startId.equals("-")) {
            startId = entries.get(0).id;
        }

        List<StreamEntry> result = new ArrayList<>();
        for (StreamEntry entry : entries) {
            if (isInRange(entry.id, startId, endId)) {
                result.add(entry);
            }
        }
        return result;
    }

    public Map<String, List<StreamEntry>> read(List<String> streamKeys, List<String> startIds, long blockMs) {
        Map<String, List<StreamEntry>> result = new HashMap<>();
        long deadline = blockMs == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + blockMs;

        while (System.currentTimeMillis() < deadline) {
            boolean found = false;
            for (int i = 0; i < streamKeys.size(); i++) {
                String key = streamKeys.get(i);
                String startId = startIds.get(i);
                if (startId.equals("$")) {
                    List<StreamEntry> entries = streams.getOrDefault(key, new ArrayList<>());
                    startId = entries.isEmpty() ? "0-0" : entries.get(entries.size() - 1).id;
                }

                List<StreamEntry> entries = streams.getOrDefault(key, new ArrayList<>());
                List<StreamEntry> matches = new ArrayList<>();
                for (StreamEntry entry : entries) {
                    if (compareIds(entry.id, startId) > 0) {
                        matches.add(entry);
                    }
                }
                if (!matches.isEmpty()) {
                    result.put(key, matches);
                    found = true;
                }
            }
            if (found || blockMs == 0) return result;

            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {}
        }
        return result;
    }

    private long getNextSequence(String streamKey, long ts) {
        List<StreamEntry> entries = streams.getOrDefault(streamKey, new ArrayList<>());
        for (int i = entries.size() - 1; i >= 0; i--) {
            String[] parts = entries.get(i).id.split("-");
            if (Long.parseLong(parts[0]) == ts) {
                return Long.parseLong(parts[1]) + 1;
            }
        }
        return 0;
    }

    private boolean isInRange(String id, String start, String end) {
        return compareIds(id, start) >= 0 && compareIds(id, end) <= 0;
    }

    private int compareIds(String id1, String id2) {
        String[] p1 = id1.split("-");
        String[] p2 = id2.split("-");
        long t1 = Long.parseLong(p1[0]);
        long s1 = Long.parseLong(p1[1]);
        long t2 = Long.parseLong(p2[0]);
        long s2 = Long.parseLong(p2[1]);
        return t1 != t2 ? Long.compare(t1, t2) : Long.compare(s1, s2);
    }

    public String type(String key) {
        return streams.containsKey(key) ? "stream" : "none";
    }
}