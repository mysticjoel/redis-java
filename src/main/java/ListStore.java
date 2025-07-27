import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ListStore {
    private final Map<String, List<String>> lists = new ConcurrentHashMap<>();
    private final Map<String, BlockingQueue<List<String>>> blockedClients = new ConcurrentHashMap<>();

    public int rpush(String key, List<String> values) {
        List<String> list = lists.computeIfAbsent(key, k -> new ArrayList<>());
        synchronized (list) {
            list.addAll(values);
            // Notify blocked clients
            BlockingQueue<List<String>> queue = blockedClients.get(key);
            if (queue != null && !list.isEmpty()) {
                List<String> result = new ArrayList<>();
                result.add(key);
                result.add(list.remove(0));
                queue.offer(result);
                if (list.isEmpty()) {
                    lists.remove(key);
                }
            }
        }
        return list.size();
    }

    public int lpush(String key, List<String> values) {
        List<String> list = lists.computeIfAbsent(key, k -> new ArrayList<>());
        synchronized (list) {
            for (String value : values) {
                list.add(0, value);
            }
            // Notify blocked clients
            BlockingQueue<List<String>> queue = blockedClients.get(key);
            if (queue != null && !list.isEmpty()) {
                List<String> result = new ArrayList<>();
                result.add(key);
                result.add(list.remove(0));
                queue.offer(result);
                if (list.isEmpty()) {
                    lists.remove(key);
                }
            }
        }
        return list.size();
    }

    public List<String> lrange(String key, int start, int stop) {
        List<String> list = lists.getOrDefault(key, new ArrayList<>());
        if (list.isEmpty()) {
            return new ArrayList<>();
        }
        int adjustedStart = start < 0 ? list.size() + start : start;
        int adjustedStop = stop < 0 ? list.size() + stop : Math.min(stop, list.size() - 1);
        adjustedStart = Math.max(adjustedStart, 0);
        adjustedStop = Math.max(adjustedStop, -1);
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
        synchronized (list) {
            for (int i = 0; i < count; i++) {
                result.add(list.remove(0));
            }
            if (list.isEmpty()) {
                lists.remove(key);
            }
        }
        return result;
    }

    public List<String> blpop(String key, long timeoutMs) {
        List<String> list = lists.getOrDefault(key, new ArrayList<>());
        BlockingQueue<List<String>> queue = blockedClients.computeIfAbsent(key, k -> new LinkedBlockingQueue<>());

        synchronized (list) {
            if (!list.isEmpty()) {
                List<String> result = new ArrayList<>();
                result.add(key);
                result.add(list.remove(0));
                if (list.isEmpty()) {
                    lists.remove(key);
                    blockedClients.remove(key);
                }
                return result;
            }
        }

        try {
            List<String> result = timeoutMs == 0 ? queue.take() : queue.poll(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (result == null) {
                blockedClients.remove(key, queue);
                return null;
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            blockedClients.remove(key, queue);
            return null;
        }
    }
}