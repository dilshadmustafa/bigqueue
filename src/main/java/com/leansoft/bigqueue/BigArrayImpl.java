package com.leansoft.bigqueue;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.dilmus.dilshad.storage.IStorageHandler;
import com.leansoft.bigqueue.page.IMappedPage;
import com.leansoft.bigqueue.page.IMappedPageFactory;
import com.leansoft.bigqueue.page.MappedPageFactoryImpl;
import com.leansoft.bigqueue.utils.Calculator;
import com.leansoft.bigqueue.utils.FileUtil;

/**
 * A big array implementation supporting sequential append and random read.
 *  
 * Main features:
 * 1. FAST : close to the speed of direct memory access, extremely fast in append only and sequential read modes,
 *           sequential append and read are close to O(1) memory access, random read is close to O(1) memory access if 
 *           data is in cache and is close to O(1) disk access if data is not in cache.
 * 2. MEMORY-EFFICIENT : automatic paging & swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple threads can concurrently read/append the array without data corruption.
 * 4. PERSISTENT - all array data is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the array data is only limited by the available disk space.
 * 
 * 
 * @author bulldog
 *
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

public class BigArrayImpl implements IBigArray {
	
	// folder name for index page
	final static String INDEX_PAGE_FOLDER = "index";
	// folder name for data page
	final static String DATA_PAGE_FOLDER = "data";
	// folder name for meta data page
	final static String META_DATA_PAGE_FOLDER = "meta_data";
	
	// 2 ^ 20 = 1024 * 1024
	final static int INDEX_ITEMS_PER_PAGE_BITS = 20; // 1024 * 1024
	// number of items per page
	final static int INDEX_ITEMS_PER_PAGE = 1 << INDEX_ITEMS_PER_PAGE_BITS;
	// 2 ^ 5 = 32
	final static int INDEX_ITEM_LENGTH_BITS = 5;
	// length in bytes of an index item
	final static int INDEX_ITEM_LENGTH = 1 << INDEX_ITEM_LENGTH_BITS; 
	// size in bytes of an index page
	final static int INDEX_PAGE_SIZE = INDEX_ITEM_LENGTH * INDEX_ITEMS_PER_PAGE; 
	
	// size in bytes of a data page
	final int DATA_PAGE_SIZE;
	
	// default size in bytes of a data page
	public final static int DEFAULT_DATA_PAGE_SIZE = 128 * 1024 * 1024;
	// minimum size in bytes of a data page
	public final static int MINIMUM_DATA_PAGE_SIZE = 32 * 1024 * 1024;
	// seconds, time to live for index page cached in memory
	final static int INDEX_PAGE_CACHE_TTL = 1000;
	// seconds, time to live for data page cached in memory
	final static int DATA_PAGE_CACHE_TTL = 1000;
	// 2 ^ 4 = 16
	final static int META_DATA_ITEM_LENGTH_BITS = 4;
	// size in bytes of a meta data page
	final static int META_DATA_PAGE_SIZE = 1 << META_DATA_ITEM_LENGTH_BITS;
	
//	private final static int INDEX_ITEM_DATA_PAGE_INDEX_OFFSET = 0;
//	private final static int INDEX_ITEM_DATA_ITEM_OFFSET_OFFSET = 8;
	private final static int INDEX_ITEM_DATA_ITEM_LENGTH_OFFSET = 12;
	// timestamp offset of an data item within an index item
	final static int INDEX_ITEM_DATA_ITEM_TIMESTAMP_OFFSET = 16;
	
	// directory to persist array data
	String arrayDirectory;
	// New field
	String m_localArrayDirPath; // to persist cached array data
	IStorageHandler m_storageHandler;
	
	// factory for index page management(acquire, release, cache)
	IMappedPageFactory indexPageFactory; 
	// factory for data page management(acquire, release, cache)
	IMappedPageFactory dataPageFactory;
	// factory for meta data page management(acquire, release, cache)
	IMappedPageFactory metaPageFactory;
	
	// only use the first page
	static final long META_DATA_PAGE_INDEX = 0;
	
	// head index of the big array, this is the read write barrier.
	// readers can only read items before this index, and writes can write this index or after
	final AtomicLong arrayHeadIndex = new AtomicLong();
	// tail index of the big array,
	// readers can't read items before this tail
	final AtomicLong arrayTailIndex = new AtomicLong();
	
	// head index of the data page, this is the to be appended data page index
	long headDataPageIndex;
	// head offset of the data page, this is the to be appended data offset
	int headDataItemOffset;
	
	// lock for appending state management
	final Lock appendLock = new ReentrantLock();
	
	// global lock for array read and write management
    final ReadWriteLock arrayReadWritelock = new ReentrantReadWriteLock();
    final Lock arrayReadLock = arrayReadWritelock.readLock();
    final Lock arrayWriteLock = arrayReadWritelock.writeLock(); 
	
	/**
	 * 
	 * A big array implementation supporting sequential write and random read,
	 * use default back data file size per page, see {@link #DEFAULT_DATA_PAGE_SIZE}.
	 * 
	 * @param arrayDir directory for array data store
	 * @param arrayName the name of the array, will be appended as last part of the array directory
	 * @throws Exception 
	 */
	public BigArrayImpl(String arrayDir, String arrayName, String localDir, IStorageHandler storageHandler) throws Exception {
		this(arrayDir, arrayName, DEFAULT_DATA_PAGE_SIZE, localDir, storageHandler);
	}
	
	/**
	 * A big array implementation supporting sequential write and random read.
	 * 
	 * @param arrayDir directory for array data store
	 * @param arrayName the name of the array, will be appended as last part of the array directory
	 * @param pageSize the back data file size per page in bytes, see minimum allowed {@link #MINIMUM_DATA_PAGE_SIZE}.
	 * @throws Exception 
	 */
	public BigArrayImpl(String arrayDir, String arrayName, int pageSize, String localDir, IStorageHandler storageHandler) throws Exception {
		arrayDirectory = arrayDir;
		if (!arrayDirectory.endsWith(File.separator)) {
			arrayDirectory += File.separator;
		}
		// append array name as part of the directory
		arrayDirectory = arrayDirectory + arrayName + File.separator;
		
		// validate directory
		if (!FileUtil.isFilenameValid(arrayDirectory)) {
			throw new IllegalArgumentException("invalid array directory : " + arrayDirectory);
		}

		// New code
		m_localArrayDirPath = localDir;
		if (!m_localArrayDirPath.endsWith(File.separator)) {
			m_localArrayDirPath += File.separator;
		}
		// append array name as part of the directory
		m_localArrayDirPath = m_localArrayDirPath + arrayName + File.separator;
		
		// validate directory
		if (!FileUtil.isFilenameValid(m_localArrayDirPath)) {
			throw new IllegalArgumentException("invalid local directory : " + m_localArrayDirPath);
		}
		// End new code
		
		// New code
		File localArrayDirPathFileObj = new File(m_localArrayDirPath);
		if (!localArrayDirPathFileObj.exists()) {
			boolean status = localArrayDirPathFileObj.mkdir();
			if (false == status)
				throw new IOException("Failed to create local array dir : " + m_localArrayDirPath + " Check if local parent directories exist");
		}
		// End new code
		
		// Storage handler
		m_storageHandler = storageHandler;
		try {
			m_storageHandler.mkdirIfAbsent(arrayDirectory);
		} catch (Exception e) {
			Path localPath = Paths.get(m_localArrayDirPath);
			Files.deleteIfExists(localPath);
			throw e;
		}
		// End Storage handler
		
		if (pageSize < MINIMUM_DATA_PAGE_SIZE) {
			throw new IllegalArgumentException("invalid page size, allowed minimum is : " + MINIMUM_DATA_PAGE_SIZE + " bytes.");
		}
		
		DATA_PAGE_SIZE = pageSize;
		
		this.commonInit();
	}
	
	// New method
	private void flushFilesAll() throws Exception {
		try {
			arrayWriteLock.lock();
			if (this.metaPageFactory != null) {
				this.metaPageFactory.flushFilesAll();
			}
			if (this.indexPageFactory != null) {
				this.indexPageFactory.flushFilesAll();
			}
			if (this.dataPageFactory != null) {
				this.dataPageFactory.flushFilesAll();
			}
			Path path = Paths.get(m_localArrayDirPath);
			Files.deleteIfExists(path);
		} finally {
			arrayWriteLock.unlock();
		}
	}
	
	// New method
	// flushes all page files excluding last page file
	public void flushFiles() throws Exception {
		try {
			arrayWriteLock.lock();
			if (this.metaPageFactory != null) {
				this.metaPageFactory.flushFilesExclude();
			}
			if (this.indexPageFactory != null) {
				this.indexPageFactory.flushFilesExclude();
			}
			if (this.dataPageFactory != null) {
				this.dataPageFactory.flushFilesExclude();
			}
		} finally {
			arrayWriteLock.unlock();
		}
	}
	
	public String getArrayDirectory() {
		return this.arrayDirectory;
	}
	
	
	void commonInit() throws Exception {
		// initialize page factories
		this.indexPageFactory = new MappedPageFactoryImpl(INDEX_PAGE_SIZE, 
				this.arrayDirectory + INDEX_PAGE_FOLDER, 
				INDEX_PAGE_CACHE_TTL, m_localArrayDirPath + INDEX_PAGE_FOLDER, m_localArrayDirPath, m_storageHandler);
		this.dataPageFactory = new MappedPageFactoryImpl(DATA_PAGE_SIZE, 
				this.arrayDirectory + DATA_PAGE_FOLDER, 
				DATA_PAGE_CACHE_TTL, m_localArrayDirPath + DATA_PAGE_FOLDER, m_localArrayDirPath, m_storageHandler);
		// the ttl does not matter here since meta data page is always cached
		this.metaPageFactory = new MappedPageFactoryImpl(META_DATA_PAGE_SIZE, 
				this.arrayDirectory + META_DATA_PAGE_FOLDER, 
				10 * 1000/*does not matter*/, m_localArrayDirPath + META_DATA_PAGE_FOLDER, m_localArrayDirPath, m_storageHandler);
		
		// initialize array indexes
		initArrayIndex();

		// initialize data page indexes
		initDataPageIndex();
	}

	// removeAll() is not supported in new bigarray. Use deletePartition() instead
	@Override
	public void removeAll() throws IOException {
		throw new IOException("removeAll() is not supported in new bigarray");
		/*
		try {
			arrayWriteLock.lock();
			this.indexPageFactory.deleteAllPages();
			this.dataPageFactory.deleteAllPages();
			this.metaPageFactory.deleteAllPages();
			//FileUtil.deleteDirectory(new File(this.arrayDirectory));
			
			this.commonInit();
		} finally {
			arrayWriteLock.unlock();
		}
		*/
	}
	
	@Override
	public void removeBeforeIndex(long index) throws Exception {
		try {
			arrayWriteLock.lock();
			
			validateIndex(index);
			
			long indexPageIndex = Calculator.div(index, INDEX_ITEMS_PER_PAGE_BITS);
			
			ByteBuffer indexItemBuffer = this.getIndexItemBuffer(index);
			long dataPageIndex = indexItemBuffer.getLong();
			
			long toRemoveIndexPageTimestamp = this.indexPageFactory.getPageFileLastModifiedTime(indexPageIndex);
			long toRemoveDataPageItemstamp = this.dataPageFactory.getPageFileLastModifiedTime(dataPageIndex);
			
			if (toRemoveIndexPageTimestamp > 0L) { 
				this.indexPageFactory.deletePagesBefore(toRemoveIndexPageTimestamp);
			}
			if (toRemoveDataPageItemstamp > 0L) {
				this.dataPageFactory.deletePagesBefore(toRemoveDataPageItemstamp);
			}
			
			// advance the tail to index
			this.arrayTailIndex.set(index);
		} finally {
			arrayWriteLock.unlock();
		}
	}
	


	@Override
	public void removeBefore(long timestamp) throws Exception {
		try {
			arrayWriteLock.lock();
			long firstIndexPageIndex = this.indexPageFactory.getFirstPageIndexBefore(timestamp);
			if (firstIndexPageIndex >= 0) {
//				long nextIndexPageIndex = firstIndexPageIndex;
//				if (nextIndexPageIndex == Long.MAX_VALUE) { //wrap
//					nextIndexPageIndex = 0L;
//				} else {
//					nextIndexPageIndex++;
//				}
				long toRemoveBeforeIndex = Calculator.mul(firstIndexPageIndex, INDEX_ITEMS_PER_PAGE_BITS);
				removeBeforeIndex(toRemoveBeforeIndex);
			}
		} catch (IndexOutOfBoundsException ex) {
			// ignore
		} finally {
			arrayWriteLock.unlock();
		}	
	}
	
	// find out array head/tail from the meta data
	void initArrayIndex() throws Exception {
		
		// cw IMappedPage metaDataPage = this.metaPageFactory.acquirePage(META_DATA_PAGE_INDEX);
		// New code
		// do a acquirePageCreateOrModifyPage() on the last page index for /meta_data/ folder file before User happens to call get(size() - 1) 
		// page index is different from big array index
		IMappedPage metaDataPage = this.metaPageFactory.acquirePageCreateOrModifyPage(META_DATA_PAGE_INDEX);
		// End New code
		ByteBuffer metaBuf = metaDataPage.getLocal(0);
		long head = metaBuf.getLong();
		long tail = metaBuf.getLong();
		
		arrayHeadIndex.set(head);
		arrayTailIndex.set(tail);
	}
	
	// find out data page head index and offset
	void initDataPageIndex() throws Exception {

		if (this.isEmpty()) {
			headDataPageIndex = 0L;
			headDataItemOffset = 0;
		} else {
			IMappedPage previousIndexPage = null;
			// New code
			IMappedPage previousDataPage = null;
			long previousDataPageIndex = -1;
			// End New code
			long previousIndexPageIndex = -1;
			try {
				long previousIndex = this.arrayHeadIndex.get() - 1;
				if (previousIndex < 0) {
					previousIndex = Long.MAX_VALUE; // wrap
				}
				previousIndexPageIndex = Calculator.div(previousIndex, INDEX_ITEMS_PER_PAGE_BITS); // shift optimization
				// cw previousIndexPage = this.indexPageFactory.acquirePage(previousIndexPageIndex);
				// New code
				// do a acquirePageCreateOrModifyPage() on the last page index for /index/ folder file before User happens to call get(size() - 1) 
				// page index is different from big array index
				previousIndexPage = this.indexPageFactory.acquirePageCreateOrModifyPage(previousIndexPageIndex);
				// End New code
				
				int previousIndexPageOffset = (int) (Calculator.mul(Calculator.mod(previousIndex, INDEX_ITEMS_PER_PAGE_BITS), INDEX_ITEM_LENGTH_BITS));
				ByteBuffer previousIndexItemBuffer = previousIndexPage.getLocal(previousIndexPageOffset);
				/* cw long*/ previousDataPageIndex = previousIndexItemBuffer.getLong();
				int previousDataItemOffset = previousIndexItemBuffer.getInt();
				int perviousDataItemLength = previousIndexItemBuffer.getInt();
				
				headDataPageIndex = previousDataPageIndex;
				headDataItemOffset = previousDataItemOffset + perviousDataItemLength;
				
				// New code
				// do a acquirePageCreateOrModifyPage() on the last page index for /data/ folder file before User happens to call get(size() - 1) 
				// page index is different from big array index
				previousDataPage = this.dataPageFactory.acquirePageCreateOrModifyPage(previousDataPageIndex);
				/// End New code
				
			} finally {
				if (previousIndexPage != null) {
					this.indexPageFactory.releasePage(previousIndexPageIndex);
				}
				// New code
				if (previousDataPage != null) {
					this.dataPageFactory.releasePage(previousDataPageIndex);
				}
				// End New code
			}
		}
	}

	/**
	 * Append the data into the head of the array
	 * @throws Exception 
	 */
	public long append(byte[] data) throws Exception {
		try {
			arrayReadLock.lock(); 
			IMappedPage toAppendDataPage = null;
			IMappedPage toAppendIndexPage = null;
			long toAppendIndexPageIndex = -1L;
			long toAppendDataPageIndex = -1L;
			
			long toAppendArrayIndex = -1L;
			
			try {
				appendLock.lock(); // only one thread can append
			
				if (this.isFull()) { // end of the world check:)
					throw new IOException("ring space of java long type used up, the end of the world!!!");
				}
				
				// prepare the data pointer
				if (this.headDataItemOffset + data.length > DATA_PAGE_SIZE) { // not enough space
					if (this.headDataPageIndex == Long.MAX_VALUE) {
						this.headDataPageIndex = 0L; // wrap
					} else {
						this.headDataPageIndex++;
					}
					this.headDataItemOffset = 0;
				}
				
				toAppendDataPageIndex = this.headDataPageIndex;
				int toAppendDataItemOffset  = this.headDataItemOffset;
				
				toAppendArrayIndex = this.arrayHeadIndex.get();
				
				// append data
				toAppendDataPage = this.dataPageFactory.acquirePageCreateOrModifyPage(toAppendDataPageIndex);
				ByteBuffer toAppendDataPageBuffer = toAppendDataPage.getLocal(toAppendDataItemOffset);
				toAppendDataPageBuffer.put(data);
				toAppendDataPage.setDirty(true);
				// update to next
				this.headDataItemOffset += data.length;
				
				toAppendIndexPageIndex = Calculator.div(toAppendArrayIndex, INDEX_ITEMS_PER_PAGE_BITS); // shift optimization
				toAppendIndexPage = this.indexPageFactory.acquirePageCreateOrModifyPage(toAppendIndexPageIndex);
				int toAppendIndexItemOffset = (int) (Calculator.mul(Calculator.mod(toAppendArrayIndex, INDEX_ITEMS_PER_PAGE_BITS), INDEX_ITEM_LENGTH_BITS));
				
				// update index
				ByteBuffer toAppendIndexPageBuffer = toAppendIndexPage.getLocal(toAppendIndexItemOffset);
				toAppendIndexPageBuffer.putLong(toAppendDataPageIndex);
				toAppendIndexPageBuffer.putInt(toAppendDataItemOffset);
				toAppendIndexPageBuffer.putInt(data.length);
				long currentTime = System.currentTimeMillis();
				toAppendIndexPageBuffer.putLong(currentTime);
				toAppendIndexPage.setDirty(true);
				
				// advance the head
				this.arrayHeadIndex.incrementAndGet();
				
				// update meta data
				IMappedPage metaDataPage = this.metaPageFactory.acquirePageCreateOrModifyPage(META_DATA_PAGE_INDEX);
				ByteBuffer metaDataBuf = metaDataPage.getLocal(0);
				metaDataBuf.putLong(this.arrayHeadIndex.get());
				metaDataBuf.putLong(this.arrayTailIndex.get());
				metaDataPage.setDirty(true);
	
			} finally {
				
				appendLock.unlock();
				
				if (toAppendDataPage != null) {
					this.dataPageFactory.releasePage(toAppendDataPageIndex);
				}
				if (toAppendIndexPage != null) {
					this.indexPageFactory.releasePage(toAppendIndexPageIndex);
				}
			}
			
			return toAppendArrayIndex;
		
		} finally {
			arrayReadLock.unlock();
		}
	}

	@Override
	public void flush() {
		try {
			arrayReadLock.lock(); 
			
//			try {
//				appendLock.lock(); // make flush and append mutually exclusive
				
				this.metaPageFactory.flush();
				this.indexPageFactory.flush();
				this.dataPageFactory.flush();
				
//			} finally {	
//				appendLock.unlock();
//			}
			
		} finally {
			arrayReadLock.unlock();
		}
		
	}

	public byte[] get(long index) throws Exception {
		try {
			arrayReadLock.lock();
			validateIndex(index);
			
			IMappedPage dataPage = null;
			long dataPageIndex = -1L;
			try {
				ByteBuffer indexItemBuffer = this.getIndexItemBuffer(index);
				dataPageIndex = indexItemBuffer.getLong();
				int dataItemOffset = indexItemBuffer.getInt();
				int dataItemLength = indexItemBuffer.getInt();
				dataPage = this.dataPageFactory.acquirePage(dataPageIndex);
				byte[] data = dataPage.getLocal(dataItemOffset, dataItemLength);
				return data;
			} finally {
				if (dataPage != null) {
					this.dataPageFactory.releasePage(dataPageIndex);
				}
			}
		} finally {
			arrayReadLock.unlock();
		}
	}
	
	public long getTimestamp(long index) throws Exception {
		try {
			arrayReadLock.lock();
			validateIndex(index);
			
			ByteBuffer indexItemBuffer = this.getIndexItemBuffer(index);
			// position to the timestamp
			int position = indexItemBuffer.position();
			indexItemBuffer.position(position + INDEX_ITEM_DATA_ITEM_TIMESTAMP_OFFSET);
			long ts = indexItemBuffer.getLong();
			return ts;
		} finally {
			arrayReadLock.unlock();
		}
	}
	
	ByteBuffer getIndexItemBuffer(long index) throws Exception {
		
		IMappedPage indexPage = null;
		long indexPageIndex = -1L;
		try {
			indexPageIndex = Calculator.div(index, INDEX_ITEMS_PER_PAGE_BITS); // shift optimization
			indexPage = this.indexPageFactory.acquirePage(indexPageIndex);
			int indexItemOffset = (int) (Calculator.mul(Calculator.mod(index, INDEX_ITEMS_PER_PAGE_BITS), INDEX_ITEM_LENGTH_BITS));
			
			ByteBuffer indexItemBuffer = indexPage.getLocal(indexItemOffset);
			return indexItemBuffer;
		} finally {
			if (indexPage != null) {
				this.indexPageFactory.releasePage(indexPageIndex);
			}
		}
	}
	
	void validateIndex(long index) {
		if (this.arrayTailIndex.get() <= this.arrayHeadIndex.get()) {
			if (index < this.arrayTailIndex.get() || index >= this.arrayHeadIndex.get()) {
				throw new IndexOutOfBoundsException();
			}
		} else {
			if (index < this.arrayTailIndex.get() && index >= this.arrayHeadIndex.get()) {
				throw new IndexOutOfBoundsException();
			}
		}
	}

	public long size() {
		try {
			arrayReadLock.lock();
			if (this.arrayTailIndex.get() <= this.arrayHeadIndex.get()) {
				return (this.arrayHeadIndex.get() - this.arrayTailIndex.get());
			} else {
				return Long.MAX_VALUE - this.arrayTailIndex.get() + 1 + this.arrayHeadIndex.get();
			}
		} finally {
			arrayReadLock.unlock();
		}
	}

	public long getHeadIndex() {
		try {
			arrayReadLock.lock();
			return arrayHeadIndex.get();
		} finally {
			arrayReadLock.unlock();
		}
	}

	public long getTailIndex() {
		try {
			arrayReadLock.lock();
			return arrayTailIndex.get();
		} finally {
			arrayReadLock.unlock();
		}
	}

	@Override
	public boolean isEmpty() {
		try {
			arrayReadLock.lock();
			return this.arrayHeadIndex.get() == this.arrayTailIndex.get();
		} finally {
			arrayReadLock.unlock();
		}
	}

	@Override
	public boolean isFull() {
		try {
			arrayReadLock.lock();
			long currentIndex = this.arrayHeadIndex.get();
			
			long nextIndex = currentIndex == Long.MAX_VALUE ? 0 : currentIndex + 1;
			return nextIndex == this.arrayTailIndex.get();
		} finally {
			arrayReadLock.unlock();
		}
	}

	//@Override as Closeable interface is not extended/commented for IBigArray interface
	public void close() throws Exception {
		flushFilesAll();
		/* cw 27-Feb-2017, as flushFilesAll() is called above, this code is not needed as flushFilesAll() also does the same thing
		try {
			arrayWriteLock.lock();
			if (this.metaPageFactory != null) {
				this.metaPageFactory.releaseCachedPages();
			}
			if (this.indexPageFactory != null) {
				this.indexPageFactory.releaseCachedPages();
			}
			if (this.dataPageFactory != null) {
				this.dataPageFactory.releaseCachedPages();
			}
		} finally {
			arrayWriteLock.unlock();
		}
		*/
	}

	@Override
	public int getDataPageSize() {
		return DATA_PAGE_SIZE;
	}

	@Override
	public long findClosestIndex(long timestamp) throws Exception {
		try {
			arrayReadLock.lock();
			long closestIndex = NOT_FOUND;
			long tailIndex = this.arrayTailIndex.get();
			long headIndex = this.arrayHeadIndex.get();
			if (tailIndex == headIndex) return closestIndex; // empty
			long lastIndex = headIndex - 1;
			if (lastIndex < 0) {
				lastIndex = Long.MAX_VALUE;
			}
			if (tailIndex < lastIndex) {
				closestIndex = closestBinarySearch(tailIndex, lastIndex, timestamp);
			} else {
				long lowPartClosestIndex = closestBinarySearch(0L, lastIndex, timestamp);
				long highPartClosetIndex = closestBinarySearch(tailIndex, Long.MAX_VALUE, timestamp);
				
				long lowPartTimestamp = this.getTimestamp(lowPartClosestIndex);
				long highPartTimestamp = this.getTimestamp(highPartClosetIndex);
				
				closestIndex = Math.abs(timestamp - lowPartTimestamp) < Math.abs(timestamp - highPartTimestamp) 
						? lowPartClosestIndex : highPartClosetIndex;
			}
			
			return closestIndex;
		} finally {
			arrayReadLock.unlock();
		}
	}
	
	private long closestBinarySearch(long low, long high, long timestamp) throws Exception {    		
        long mid;
        long sum = low + high;
        if (sum < 0) { // overflow
        	BigInteger bigSum = BigInteger.valueOf(low);
        	bigSum = bigSum.add(BigInteger.valueOf(high));
        	mid = bigSum.shiftRight(1).longValue();
        } else {
        	mid = sum / 2;
        }
        
    	long midTimestamp = this.getTimestamp(mid);
    	
    	if (midTimestamp < timestamp) {
    		long nextLow = mid + 1;
    		if (nextLow >= high) {
    			return high;
    		}
    		return closestBinarySearch(nextLow, high, timestamp);
    	} else if (midTimestamp > timestamp) {
    		long nextHigh = mid - 1;
    		if (nextHigh <= low) {
    			return low;
    		}
    		return closestBinarySearch(low, nextHigh, timestamp);
    	} else {
    		return mid;
    	}
	}

	@Override
	public long getBackFileSize() throws IOException {
		try {
			arrayReadLock.lock();
			
			return this._getBackFileSize();
			
		} finally {
			arrayReadLock.unlock();
		}
	}

	@Override
	public void limitBackFileSize(long sizeLimit) throws Exception {
		if (sizeLimit < INDEX_PAGE_SIZE + DATA_PAGE_SIZE) {
			return; // ignore, one index page + one data page are minimum for big array to work correctly
		}
		
		long backFileSize = this.getBackFileSize();
		if (backFileSize <= sizeLimit) return; // nothing to do
		
		long toTruncateSize = backFileSize - sizeLimit;
		if (toTruncateSize < DATA_PAGE_SIZE) {
			return; // can't do anything
		}
		
		try {
			arrayWriteLock.lock();
		
			// double check
			backFileSize = this._getBackFileSize();
			if (backFileSize <= sizeLimit) return; // nothing to do
			
			toTruncateSize = backFileSize - sizeLimit;
			if (toTruncateSize < DATA_PAGE_SIZE) {
				return; // can't do anything
			}
			
			long tailIndex = this.arrayTailIndex.get();
			long headIndex = this.arrayHeadIndex.get();
			long totalLength = 0L;
			while(true) {
				if (tailIndex == headIndex) break;
				totalLength += this.getDataItemLength(tailIndex);
				if (totalLength > toTruncateSize) break;
				
				if (tailIndex == Long.MAX_VALUE) {
					tailIndex = 0;
				} else {
					tailIndex++;
				}
				if (Calculator.mod(tailIndex, INDEX_ITEMS_PER_PAGE_BITS) == 0) { // take index page into account
					totalLength += INDEX_PAGE_SIZE;
				}
			}
			this.removeBeforeIndex(tailIndex);
		} finally {
			arrayWriteLock.unlock();
		}
		
	}

	@Override
	public int getItemLength(long index) throws Exception {
		try {
			arrayReadLock.lock();
			validateIndex(index);
			
			return getDataItemLength(index);

		} finally {
			arrayReadLock.unlock();
		}
	}
	
	private int getDataItemLength(long index) throws Exception {
		
		ByteBuffer indexItemBuffer = this.getIndexItemBuffer(index);
		// position to the data item length
		int position = indexItemBuffer.position();
		indexItemBuffer.position(position + INDEX_ITEM_DATA_ITEM_LENGTH_OFFSET);
		int length = indexItemBuffer.getInt();
		return length;
	}
	
	// inner getBackFileSize
	private long _getBackFileSize() throws IOException {	
		return this.indexPageFactory.getBackPageFileSize() + this.dataPageFactory.getBackPageFileSize();
	}
}
