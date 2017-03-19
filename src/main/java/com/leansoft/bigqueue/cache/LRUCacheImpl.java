package com.leansoft.bigqueue.cache;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

/**
 * Simple and thread-safe LRU cache implementation, 
 * supporting time to live and reference counting for entry.
 * 
 * in current implementation, entry expiration and purge(mark&sweep) is triggered by put operation,
 * and resource closing after mark&sweep is done in async way.  
 * 
 * @author bulldog
 *
 * @param <K> key
 * @param <V> value
 */

/*
 * This release of BigArray is a modification of the original BigArray version 0.7.0 by Leansoft Technology,
 * https://github.com/bulldog2011/bigqueue, released under Apache License Version 2.0.

 * This modified BigArray is also released under Apache License Version 2.0.

 * BigArray modifications:-

 * Dilshad Mustafa has modified the original BigArray and implemented the following functionality:-

 * (1) All the page files are stored in a storage system accessed through the IStorageHandler.java interface.
 * (2) Memory-mapped files are from a locally cached file in the local file system.
 * (3) Multiple BigArray objects (refer constructor) with same arrayName should not be created with the 
 *     same localDir (local directory path) as locally cached files should be kept separate.
 * (4) Multiple BigArray objects (refer constructor) with same arrayName should not be created with the 
 *     same storageDir (storage directory path) as this will result in overwriting of page files.
 * 
 * Page handling by this modified BigArray:-
 * 
 * (1) In BigArray, all Page files are written only once to the storage system to implement Write Once 
 *     Read Many (WORM) design pattern.
 * (2) This big array (the associated array folder) should not be modified after this BigArrayImpl object 
 *     is closed [.close()] to keep the WORM design pattern.
 * (3) Write Once Read Many (WORM) design pattern is built within the modified BigArray data structure itself 
 *     and does not depend on IStorageHandler implementation. 
 * (4) flushFiles() method can be used to flush the locally cached files (those modified), excluding the 
 *     last page file, into the storage system. User can call flushFiles() to release memory if needed.    
 * (5) New field m_unfilledPage in MappedPageFactoryImpl.java is used to keep track of the last page file 
 *     to prevent it from getting removed from the cache. This way the last page file (whether it is unfilled 
 *     / partially filled or fully filled) will be written, if modified [.append()], only once to the storage 
 *     system at the time of closing the Big Array.   
 * (6) If another BigArrayImpl object is created for this same array folder to do an append(), then the last 
 *     page file (for data, index, meta_data) before the append() will be written again to the storage system 
 *     if that page file is not fully filled. 
 *
 *     Because during append(), setDirty(true) is set on the last page file (for data, index, meta_data) if that 
 *     page file is not fully filled. The last page file is referred to as unfilled page (m_unfilledPage in 
 *     MappedPageFactoryImpl.java). 

 *     So the big array (the associated array folder) should not be modified, either through same or another 
 *     BigArrayImpl object, after it is created and closed [.close()] to keep the WORM design pattern.
 *     
 */

public class LRUCacheImpl<K, V extends Closeable> implements ILRUCache<K, V> {
	
	private final static Logger logger = Logger.getLogger(LRUCacheImpl.class);
	
	public static final long DEFAULT_TTL = 10 * 1000; // milliseconds
	
	private final Map<K, V> map;
	private final Map<K, TTLValue> ttlMap;
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock(); 
	
	private static final ExecutorService executorService = Executors.newCachedThreadPool();
	
	private final Set<K> keysToRemove = new HashSet<K>();
	
	public LRUCacheImpl() {
		map = new HashMap<K, V>();
		ttlMap = new HashMap<K, TTLValue>();
	}

	// New method
	public LinkedList<K> getKeys() {
		try {
			readLock.lock();
		    LinkedList<K> col = new LinkedList<K>();
		    for(K k : map.keySet()) {
		    	col.add(k);
		    }
		    return col;
		} finally {
			readLock.unlock();
		}

	}
	
	// Modified
	public void put(K key, V value, long ttlInMilliSeconds, V valueToExclude) throws Exception {
		Collection<V> valuesToClose = null;
		try {
			writeLock.lock();
			// trigger mark&sweep
			valuesToClose = markAndSweep();
			if (valuesToClose != null && valuesToClose.contains(value)) { // just be cautious
				valuesToClose.remove(value);
			}
			
			if (valuesToClose != null && valueToExclude != null && valuesToClose.contains(valueToExclude)) { // just be cautious
				valuesToClose.remove(valueToExclude);
			}
			
			map.put(key, value);
			TTLValue ttl = new TTLValue(System.currentTimeMillis(), ttlInMilliSeconds);
			ttl.refCount.incrementAndGet();
			ttlMap.put(key, ttl);
		} finally {
			writeLock.unlock();
		}
		if (valuesToClose != null && valuesToClose.size() > 0) {
			if (logger.isDebugEnabled()) { 
				int size = valuesToClose.size();
				logger.info("Mark&Sweep found " + size + (size > 1 ? " resources":" resource")  + " to close.");
			}
			// close resource asynchronously
			// replaced with new code below executorService.execute(new ValueCloser<V>(valuesToClose));
			// New code
			ValueCloser<V> vc = new ValueCloser<V>(valuesToClose);
			vc.doInThisThread();
			// End New code
		}
	}

	// Modified
	public void put(K key, V value, V valueToExclude) throws Exception {
		this.put(key, value, DEFAULT_TTL, valueToExclude);
	}
	
	/**
	 * A lazy mark and sweep,
	 * 
	 * a separate thread can also do this.
	 */
	private Collection<V> markAndSweep() {
		Collection<V> valuesToClose = null;
		keysToRemove.clear();
		Set<K> keys = ttlMap.keySet();
		long currentTS = System.currentTimeMillis();
		for(K key: keys) {
			TTLValue ttl = ttlMap.get(key);
			if (ttl.refCount.get() <= 0 && (currentTS - ttl.lastAccessedTimestamp.get()) > ttl.ttl) { // remove object with no reference and expired
				keysToRemove.add(key);
			}
		}
		
		if (keysToRemove.size() > 0) {
			valuesToClose = new HashSet<V>();
			for(K key : keysToRemove) {
				V v = map.remove(key);
				valuesToClose.add(v);
				ttlMap.remove(key);
			}
		}
		
		return valuesToClose;
	}

	public V get(K key) {
		try {
			readLock.lock();
			TTLValue ttl = ttlMap.get(key);
			if (ttl != null) {
				// Since the resource is acquired by calling thread,
				// let's update last accessed timestamp and increment reference counting
				ttl.lastAccessedTimestamp.set(System.currentTimeMillis());
				ttl.refCount.incrementAndGet();
			}
			return map.get(key);
		} finally {
			readLock.unlock();
		}
	}
	
	private static class TTLValue {
		AtomicLong lastAccessedTimestamp; // last accessed time
		AtomicLong refCount = new AtomicLong(0);
		long ttl;
		
		public TTLValue(long ts, long ttl) {
			this.lastAccessedTimestamp = new AtomicLong(ts);
			this.ttl = ttl;
		}
	}
	
	private static class ValueCloser<V extends Closeable> implements Runnable {
		Collection<V> valuesToClose;
		
		public ValueCloser(Collection<V> valuesToClose) {
			this.valuesToClose = valuesToClose;
		}
		
		// New code
		public void doInThisThread() throws Exception {
			int size = valuesToClose.size();
			for(V v : valuesToClose) {
				if (v != null) {
					v.close();
				}
			}
			if (logger.isDebugEnabled()) {
				logger.debug("doInThisThread() ResourceCloser closed " + size + (size > 1 ? " resources.":" resource."));
			}
		}
		// End New code
		
		public void run() {
			int size = valuesToClose.size();
			for(V v : valuesToClose) {
				try {
					if (v != null) {
						v.close();
					}
				} catch (IOException e) {
					// close quietly
				}
			}
			if (logger.isDebugEnabled()) {
				logger.debug("ResourceCloser closed " + size + (size > 1 ? " resources.":" resource."));
			}
		}
	}

	public void release(K key) {
		try {
			readLock.lock();
			TTLValue ttl = ttlMap.get(key);
			if (ttl != null) {
				// since the resource is released by calling thread
				// let's decrement the reference counting
				ttl.refCount.decrementAndGet();
			}
		} finally {
			readLock.unlock();
		}
	}

	public int size() {
		try {
			readLock.lock();
			return map.size();
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public void removeAll()  throws IOException {
		try {
			writeLock.lock();
			
			Collection<V> valuesToClose = new HashSet<V>();
			valuesToClose.addAll(map.values());
			
			if (valuesToClose != null && valuesToClose.size() > 0) {
				// close resource synchronously
				for(V v : valuesToClose) {
					v.close();
				}
			}
			map.clear();
			ttlMap.clear();
			
		} finally {
			writeLock.unlock();
		}
		
	}

	// New method
	@Override
	public void removeExclude(V valueToExclude) throws Exception {
		try {
			writeLock.lock();
			
			Collection<V> valuesToClose = new HashSet<V>();
			valuesToClose.addAll(map.values());
			
			if (valuesToClose != null && valuesToClose.size() > 0 && valueToExclude != null) {
				if (valuesToClose.contains(valueToExclude)) {
						valuesToClose.remove(valueToExclude);
				}
			}
				
			if (valuesToClose != null && valuesToClose.size() > 0) {
				// close resource synchronously
				for(V v : valuesToClose) {
					v.close();
				}
			}
			
			Set<Entry<K, V>> entries = map.entrySet();
			K keyForValueToExclude = null;
			for (Entry<K, V> entry : entries) {
				if (entry.getValue() == valueToExclude) {
					keyForValueToExclude = entry.getKey();
					break;
				}
			}
			
			if (keyForValueToExclude != null) {
				map.clear();
				map.put(keyForValueToExclude, valueToExclude);
				
				TTLValue ttlValueToExclude = ttlMap.get(keyForValueToExclude);
				ttlMap.clear();
				ttlMap.put(keyForValueToExclude, ttlValueToExclude);
			}
						
		} finally {
			writeLock.unlock();
		}
		
	}
	
	@Override
	public V remove(K key) throws IOException {
		try {
			writeLock.lock();
			ttlMap.remove(key);
			V value = map.remove(key);
			if (value != null) {
				// close synchronously
				value.close();
			}
			return value;
		} finally {
			writeLock.unlock();
		}
		
	}

	@Override
	public Collection<V> getValues() {
		try {
			readLock.lock();
		    Collection<V> col = new ArrayList<V>();
		    for(V v : map.values()) {
		    	col.add(v);
		    }
		    return col;
		} finally {
			readLock.unlock();
		}
	}

}
