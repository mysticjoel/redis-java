import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ListStore {
    private final Map<String, List<String>> lists = new ConcurrentHashMap<>();

    public int rpush(String key, List<String> values) {
        List<String> list = lists.computeIfAbsent(key, k -> new ArrayList<>());
        list.addAll(values);
        return list.size();
    }

    public int lpush(String key, List<String> values) {
        List<String> list = lists.computeIfAbsent(key, k -> new ArrayList<>());
        // Prepend values in the order they are provided
        for (String value : values) {
            list.add(0, value);
        }
        return list.size();
    }

    public List<String> lrange(String key, int start, int stop) {
        List<String> list = lists.getOrDefault(key, new ArrayList<>());
        if (list.isEmpty()) {
            return new ArrayList<>();
        }
        int adjustedStop = stop < 0 ? list.size() + stop : Math.min(stop, list.size() - 1);
        int adjustedStart = Math.max(start, 0);
        if (adjustedStart > adjustedStop || adjustedStart >= list.size()) {
            return new ArrayList<>();
        }
        return new ArrayList<>(list.subList(adjustedStart, adjustedStop + 1));
    }

    public int llen(String key) {
        return lists.getOrDefault(key, new ArrayList<>()).size();
    }

    public List<String> lpop(String key, int count) {
        List<String> list = lists.getOrDefault(key, new ArrayList<>());
        if (list.isEmpty()) {
            return null;
        }
        List<String> result = new ArrayList<>();
        count = Math.min(count, list.size());
        for (int i = 0; i < count; i++) {
            result.add(list.remove(0));
        }
        if (list.isEmpty()) {
            lists.remove(key);
        }
        return result;
    }

    public List<String> blpop(String key, long timeoutMs) {
        List<String> list = lists.getOrDefault(key, k -> new ArrayList<>());
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (list.isEmpty() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {}
            list = lists.getOrDefault(key, new ArrayList<>());
        }
        if (list.isEmpty()) {
            return null;
        }
        List<String> result = new ArrayList<>();
        result.add(list.remove(0));
        if (list.isEmpty()) {
            lists.remove(key);
        }
        return result;
    }
}