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
        for (int i = values.size() - 1; i >= 0; i--) {
            list.add(0, values.get(i));
        }
        return list.size();
    }

    public List<String> lrange(String key, int start, int end) {
        List<String> list = lists.get(key);
        if (list == null || list.isEmpty()) return new ArrayList<>();

        int size = list.size();
        if (start < 0) start = size + start;
        if (end < 0) end = size + end;
        start = Math.max(0, start);
        end = Math.min(size - 1, end);

        if (start > end || start >= size) return new ArrayList<>();
        return new ArrayList<>(list.subList(start, end + 1));
    }

    public int llen(String key) {
        List<String> list = lists.get(key);
        return list != null ? list.size() : 0;
    }

    public List<String> lpop(String key, int count) {
        List<String> list = lists.get(key);
        if (list == null || list.isEmpty()) return null;

        count = Math.min(count, list.size());
        List<String> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(list.remove(0));
        }
        if (list.isEmpty()) lists.remove(key);
        return result;
    }

    public List<String> blpop(String key, long timeoutMs) {
        long deadline = timeoutMs == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            List<String> result = lpop(key, 1);
            if (result != null) return result;
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {}
        }
        return null;
    }
}