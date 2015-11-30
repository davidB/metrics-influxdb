package metrics_influxdb;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

public class SortedMaps {
    public static <K, V> SortedMap<K, V> empty() {
        return new TreeMap<>();
    }
    
    public static <K, V> SortedMap<K, V> singleton(K key, V value) {
        return new TreeMap<>(Collections.singletonMap(key, value));
    }
}
