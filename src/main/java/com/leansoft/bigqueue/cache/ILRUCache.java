package com.leansoft.bigqueue.cache;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;

/**
 * LRU cache ADT
 * 
 * @author bulldog
 *
 * @param <K> the key
 * @param <V> the value
 */
public interface ILRUCache<K, V extends Closeable> {
	
	public LinkedList<K> getKeys();
	
	/**
	 * Put a keyed resource with specific ttl into the cache
	 * 
	 * This call will increment the reference counter of the keyed resource.
	 * 
	 * @param key the key of the cached resource
	 * @param value the to be cached resource
	 * @param ttlInMilliSeconds time to live in milliseconds
	 */
	// Modified
	public void put(final K key, final V value, final long ttlInMilliSeconds, final V valueToExclude) throws Exception;
	
	/**
	 * Put a keyed resource with default ttl into the cache
	 * 
	 * This call will increment the reference counter of the keyed resource.
	 * 
	 * @param key the key of the cached resource
	 * @param value the to be cached resource
	 */
	// Modified
	public void put(final K key, final V value, final V valueToExclude) throws Exception;
	
	
	/**
	 * Get a cached resource with specific key
	 * 
	 * This call will increment the reference counter of the keyed resource.
	 * 
	 * @param key the key of the cached resource
	 * @return cached resource if exists
	 */
	public V get(final K key);
	
	/**
	 * Release the cached resource with specific key
	 * 
	 * This call will decrement the reference counter of the keyed resource.
	 * 
	 * @param key
	 */
	public void release(final K key);
	
	/**
	 * Remove the resource with specific key from the cache and close it synchronously afterwards.
	 * 
	 * @param key the key of the cached resource
	 * @return the removed resource if exists
	 * @throws IOException exception thrown if there is any IO error
	 */
	public V remove(final K key) throws IOException;
	
	/**
	 * Remove all cached resource from the cache and close them asynchronously afterwards.
	 * 
	 * @throws IOException exception thrown if there is any IO error
	 */
	public void removeAll() throws IOException;
	
	// New method
	public void removeExclude(V valueToExclude) throws Exception;
	
	/**
	 * The size of the cache, equals to current total number of cached resources.
	 * 
	 * @return the size of the cache
	 */
	public int size();
	
	/**
	 * All values cached
	 * @return a collection
	 */
	public Collection<V> getValues();
}
