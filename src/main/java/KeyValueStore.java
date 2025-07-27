import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KeyValueStore {
    private static class ValueWithExpiry {
        String value;
        long expiryTimeMillis;

        ValueWithExpiry(String value, long expiryTimeMillis) {
            this.value = value;
            this.expiryTimeMillis = expiryTimeMillis;
        }
    }

    private final Map<String, ValueWithExpiry> store = new HashMap<>();

    public void set(String key, String value, long expiryTime) {
        store.put(key, new ValueWithExpiry(value, expiryTime));
    }

    public String get(String key) {
        ValueWithExpiry stored = store.get(key);
        if (stored == null || (stored.expiryTimeMillis != Long.MAX_VALUE &&
                System.currentTimeMillis() > stored.expiryTimeMillis)) {
            store.remove(key);
            return null;
        }
        return stored.value;
    }

    public long increment(String key) {
        String value = get(key);
        long newValue;
        if (value != null) {
            try {
                newValue = Long.parseLong(value) + 1;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Value is not an integer");
            }
        } else {
            newValue = 1;
        }
        set(key, String.valueOf(newValue), Long.MAX_VALUE);
        return newValue;
    }

    public Set<String> keys() {
        return store.keySet();
    }

    public String type(String key) {
        return store.containsKey(key) ? "string" : "none";
    }
}