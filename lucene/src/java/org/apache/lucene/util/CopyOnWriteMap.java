package org.apache.lucene.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A simple CopyOnWrite Map implementation backed by a {@link HashMap}
 */
public final class CopyOnWriteMap<K, V> implements Map<K, V> {
  /*
     We're publishing map unsafely, but the map is all-round-final itself, so while
     read threads might see a stale map, they always see internally consistent map.
   */
  private final Object sync = new Object();
  private Map<K, V> map = Collections.emptyMap();
  
  public CopyOnWriteMap() {
  }
 
  public boolean isEmpty() {
    return map.isEmpty();
  }

  public int size() {
    return map.size();
  }

  public Collection<V> values() {
    return map.values();
  }

  public Set<K> keySet() {
    return map.keySet();
  }

  public Set<Entry<K, V>> entrySet() {
    return map.entrySet();
  }

  public V get(Object key) {
    return map.get(key);
  }

  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  public void clear() {
    synchronized (sync) {
      map = Collections.emptyMap();
    }
  }
  
  private Map<K, V> newHashMap(Map<K, V> map2) {
    return new HashMap<K,V>(map2) ;
  }


  public void putAll(Map<? extends K, ? extends V> map) {
    synchronized (sync) {
      Map<K, V> temporary = new HashMap<K,V>(this.map);
      this.map = Collections.unmodifiableMap(temporary);
    }
  }

  public V remove(Object o) {
    synchronized (sync) {
      Map<K, V> temporary = newHashMap(map);
      V value = temporary.remove(o);
      map = Collections.unmodifiableMap(temporary);
      return value;
    }
  }

 
  public V put(K k, V v) {
    synchronized (sync) {
      Map<K, V> temporary = newHashMap(map);
      V value = temporary.put(k, v);
      map = Collections.unmodifiableMap(temporary);
      return value;
    }
  }

  public String toString() {
    return map.toString();
  }
}
