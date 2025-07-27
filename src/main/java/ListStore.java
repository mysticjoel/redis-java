import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ListStore {
    private final Map<String, List<String>> lists = new ConcurrentHashMap<>();
    private final Map<String, BlockingQueue<Runnable>> blockedClients = new ConcurrentHashMap<>();

    public int rpush(String key, List<String> values) {
        List<String> list = lists.computeIfAbsent(key, k -> new ArrayList<>());
        synchronized (list) {
            list.addAll(values);
            int sizeAfterPush = list.size();
            System.out.println("RPUSH key: " + key + ", values: " + values + ", list: " + list);
            BlockingQueue<Runnable> queue = blockedClients.get(key);
            if (queue != null) {
                while (!list.isEmpty() && !queue.isEmpty()) {
                    Runnable client = queue.poll();
                    if (client != null) {
                        List<String> result = new ArrayList<>();
                        result.add(key);
                        result.add(list.remove(0));
                        System.out.println("RPUSH notifying with result: " + result);
                        client.run(); // Signal client to process result
                    }
                }
                if (list.isEmpty()) {
                    lists.remove(key);
                    blockedClients.remove(key);
                }
            }
            return sizeAfterPush;
        }
    }

    public int lpush(String key, List<String> values) {
        List<String> list = lists.computeIfAbsent(key, k -> new ArrayList<>());
        synchronized (list) {
            for (String value : values) {
                list.add(0, value);
            }
            int sizeAfterPush = list.size();
            System.out.println("LPUSH key: " + key + ", values: " + values + ", list: " + list);
            BlockingQueue<Runnable> queue = blockedClients.get(key);
            if (queue != null) {
                while (!list.isEmpty() && !queue.isEmpty()) {
                    Runnable client = queue.poll();
                    if (client != null) {
                        List<String> result = new ArrayList<>();
                        result.add(key);
                        result.add(list.remove(0));
                        System.out.println("LPUSH notifying with result: " + result);
                        client.run();
                    }
                }
                if (list.isEmpty()) {
                    lists.remove(key);
                    blockedClients.remove(key);
                }
            }
            return sizeAfterPush;
        }
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
                blockedClients.remove(key);
            }
        }
        return result;
    }

    public List<String> blpop(String key, long timeoutMs, Runnable callback) {
        System.out.println("BLPOP key: " + key + ", timeoutMs: " + timeoutMs + ", thread: " + Thread.currentThread().getName());
        BlockingQueue<Runnable> queue = blockedClients.computeIfAbsent(key, k -> new LinkedBlockingQueue<>());
        List<String> list = lists.getOrDefault(key, new ArrayList<>());

        synchronized (list) {
            if (!list.isEmpty()) {
                List<String> result = new ArrayList<>();
                result.add(key);
                result.add(list.remove(0));
                System.out.println("BLPOP immediate result: " + result);
                if (list.isEmpty()) {
                    lists.remove(key);
                    blockedClients.remove(key);
                }
                return result;
            }
            queue.offer(callback);
            System.out.println("BLPOP registered in queue for key: " + key + ", queue size: " + queue.size());
            return null; // Indicate blocking
        }
    }
}