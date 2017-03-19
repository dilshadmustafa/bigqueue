package com.leansoft.bigqueue.page;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.dilmus.dilshad.storage.IStorageHandler;
import com.leansoft.bigqueue.cache.ILRUCache;
import com.leansoft.bigqueue.cache.LRUCacheImpl;
import com.leansoft.bigqueue.utils.FileUtil;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * Mapped mapped page resource manager,
 * responsible for the creation, cache, recycle of the mapped pages. 
 * 
 * automatic paging and swapping algorithm is leveraged to ensure fast page fetch while
 * keep memory usage efficient at the same time.  
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

public class MappedPageFactoryImpl implements IMappedPageFactory {
	
	private final static Logger logger = Logger.getLogger(MappedPageFactoryImpl.class);
	
	private int pageSize;
	private String pageDir;
	private File pageDirFile;
	private String pageFile;
	private long ttl;
	
	// New fields
	private String m_localPageDirPath;
	private File m_localPageDirPathFileObj;
	private String m_localPageFilePath;
	private String m_localArrayDirPath;
	private File m_localArrayDirPathFileObj;
	private IStorageHandler m_storageHandler;

	private final Object mapLock = new Object();
	private final Map<Long, Object> pageCreationLockMap = new HashMap<Long, Object>();
	
	public static final String PAGE_FILE_NAME = "page";
	public static final String PAGE_FILE_SUFFIX = ".dat";
	
	private ILRUCache<Long, MappedPageImpl> cache;
	
	// New field, used to keep track of the last page file to prevent it from getting removed from the cache
	// This way the last page file (whether its unfilled / partially filled or fully filled) will be written 
	// only once to the storage system at the time of closing the Big Array
	private MappedPageImpl m_unfilledPage = null;
	
	// New method
	private static int deleteFile(String filePath) {
		
		File f = new File(filePath);
		
		if (f.exists() == false)
			return 0;
		if (f.isFile()){
			System.out.println("deleteFile(str) Deleting file : " + filePath);
			f.delete();
		}
		return 0;
	}
	
	// New method
	public void flushFilesAll() throws Exception {
		
		// New code
		cache.removeAll(); // this also automatically calls close() of MappedPageImpl class
		Path path = Paths.get(m_localPageDirPath);
		Files.deleteIfExists(path);
		// End New code
		
		/* New code - approach-1
		LinkedList<Long> lst = cache.getKeys();
		for (Long lon : lst) {
			// remove cached page from lru cache 
			if (lon.longValue() >= 0) {
				IMappedPage imp = cache.get(lon.longValue());
				boolean isDirty = imp.isDirty();
				cache.remove(lon.longValue()); // this also automatically calls close()->flush(), unmap() of MappedPageImpl class
				String fileNameLocal = this.getLocalFileNameByIndex(lon.longValue());
				Path localPath = Paths.get(fileNameLocal);
				
				if (isDirty) {
					// copy cached page file from local_dir to storage_dir
					// From file
					// Previous works FileInputStream fis = new FileInputStream(fileNameLocal);
					// To file
					String storageFilePath = this.getFileNameByIndex(lon.longValue());
					// Previous works FileOutputStream fos = new FileOutputStream(storageFilePath);
					
					Path storagePath = Paths.get(storageFilePath);
					Files.deleteIfExists(storagePath);
					Files.copy(localPath, storagePath, StandardCopyOption.REPLACE_EXISTING); // StandardCopyOption.ATOMIC_MOVE throws exception
					
					// Previous works
					//byte buf[] = new byte[64 * 1024 * 1024];
					//int len = 0;
					//while ((len = fis.read(buf, 0, 64 * 1024 * 1024)) > 0)
					//	fos.write(buf, 0, len);
					//fos.close();
					//fis.close();
					
				}
				// delete cached page file fileNameLocal 
				Files.delete(localPath);
				// Previous works deleteFile(fileNameLocal);
			}
		} // End for
		*/
		// End New code - approach-1
	}
	
	// New method
	public void flushFilesExclude() throws Exception {
		// New code
		cache.removeExclude(m_unfilledPage);
		// End New code
	}
	
	public MappedPageFactoryImpl(int pageSize, String pageDir, long cacheTTL, String localPageDirPath, String localArrayDirPath, IStorageHandler storageHandler) throws Exception {
		this.pageSize = pageSize;
		this.pageDir = pageDir;
		this.ttl = cacheTTL;
		
		/* Current works
		this.pageDirFile = new File(this.pageDir);
		if (!pageDirFile.exists()) {
			pageDirFile.mkdirs();
		}
		*/
		
		if (!this.pageDir.endsWith(File.separator)) {
			this.pageDir += File.separator;
		}
		this.pageFile = this.pageDir + PAGE_FILE_NAME + "-"; 
		
		// New code
		m_localArrayDirPath = localArrayDirPath;
		m_localArrayDirPathFileObj = new File(m_localArrayDirPath);
		/* Already done in BigArray constructor
		if (!m_localArrayDirFile.exists()) {
			boolean status = m_localArrayDirFile.mkdir();
			if (false == status)
				throw new IOException("Failed to create local array dir : " + m_localArrayDir + " Check if local parent directories exist");
		}
		*/

		m_localPageDirPath = localPageDirPath;
		m_localPageDirPathFileObj = new File(m_localPageDirPath);
		if (!m_localPageDirPathFileObj.exists()) {
			// need to create local array folder as well as local page folder
			// this is the first time local array folder is created
			// so need to use .mkdirs()
			// Current works m_localPageDirFile.mkdirs();
			
			boolean status = m_localPageDirPathFileObj.mkdir();
			if (false == status)
				throw new IOException("Failed to create local page dir : " + m_localPageDirPath + " Check if local parent directories exist");			
		}
		if (!m_localPageDirPath.endsWith(File.separator)) {
			m_localPageDirPath += File.separator;
		}
		m_localPageFilePath = m_localPageDirPath + "local_" + PAGE_FILE_NAME + "-"; 		
		// End new code
		
		// Storage handler
		m_storageHandler = storageHandler;
		try {
			m_storageHandler.mkdirIfAbsent(this.pageDir);
		} catch (Exception e) {
			Path localPath = Paths.get(m_localPageDirPath);
			Files.deleteIfExists(localPath);
			throw e;
		}
		// End Storage handler
		
		this.cache = new LRUCacheImpl<Long, MappedPageImpl>();
	}
	
	public IMappedPage acquirePage(long index) throws Exception {
		MappedPageImpl mpi = cache.get(index);
		if (mpi == null) { // not in cache, need to create one
			try {
				Object lock = null;
				synchronized(mapLock) {
					if (!pageCreationLockMap.containsKey(index)) {
						pageCreationLockMap.put(index, new Object());
					}
					lock = pageCreationLockMap.get(index);
				}
				synchronized(lock) { // only lock the creation of page index
					mpi = cache.get(index); // double check
					if (mpi == null) {
						/* New code - approach-1
						LinkedList<Long> lst = cache.getKeys();
						for (Long lon : lst) {
							// remove cached pages from lru cache 
							if (lon.longValue() >= 0) {
								IMappedPage imp = cache.get(lon.longValue());
								boolean isDirty = imp.isDirty();
								cache.remove(lon.longValue()); // this also automatically calls close()->flush(), unmap() of MappedPageImpl class
								String fileNameLocal = this.getLocalFileNameByIndex(lon.longValue());
								Path localPath = Paths.get(fileNameLocal);
								
								if (isDirty) {
									// copy page file from local_dir to storage_dir
									// From file
									// Previous works FileInputStream fis = new FileInputStream(fileNameLocal);
									// To file
									String storageFilePath = this.getFileNameByIndex(lon.longValue());
									// Previous works FileOutputStream fos = new FileOutputStream(storageFilePath);
									
									Path storagePath = Paths.get(storageFilePath);
									Files.deleteIfExists(storagePath);
									Files.copy(localPath, storagePath, StandardCopyOption.REPLACE_EXISTING); // StandardCopyOption.ATOMIC_MOVE throws exception
									
									// Previous works
									//byte buf[] = new byte[64 * 1024 * 1024];
									//int len = 0;
									//while ((len = fis.read(buf, 0, 64 * 1024 * 1024)) > 0)
									//	fos.write(buf, 0, len);
									//fos.close();
									//fis.close();
									
								}
								// delete cached page file fileNameLocal 
								Files.delete(localPath);
								// Previous works deleteFile(fileNameLocal);
							}
						} // End for						
						*/
						// End New code - approach-1
						
						// New code
						// Previous flushFiles() on BigArray would have deleted local array folder
						// and local page folders (index, data, meta-data)
						// so need to recreate local array folder and local page folder in m_localPageDirPath dir path
						// thats why .mkdirs() is used here
						if (!m_localArrayDirPathFileObj.exists()) {
							boolean status = m_localArrayDirPathFileObj.mkdir();
							if (false == status)
								throw new IOException("Failed to create local array dir : " + m_localArrayDirPath + " Check if local parent directories exist");
						}
						
						if (!m_localPageDirPathFileObj.exists()) {
							// Current works m_localPageDirFile.mkdirs();
							
							boolean status = m_localPageDirPathFileObj.mkdir();
							if (false == status)
								throw new IOException("Failed to create local page dir : " + m_localPageDirPath + " Check if local parent directories exist");			
						}
					
						/* Current works
						// copy existing page file from storage dir to local dir
						// From file
						String storageFilePath = this.getFileNameByIndex(index);
						// Previous works File f = new File(storageFilePath);
						Path storagePath = Paths.get(storageFilePath);
						
						// Previous works if (f.exists()) {
						if (Files.exists(storagePath, LinkOption.NOFOLLOW_LINKS)) {							
							// Previous works FileInputStream fis = new FileInputStream(storageFilePath);
	
							// To file
							String fileNameLocal = this.getLocalFileNameByIndex(index);
							// Previous works FileOutputStream fos = new FileOutputStream(fileNameLocal);

							Path localPath = Paths.get(fileNameLocal);
							if (Files.exists(localPath, LinkOption.NOFOLLOW_LINKS))
								logger.warn("File : " + fileNameLocal + " already exists. Why? How? Overwriting");
							Files.copy(storagePath, localPath, StandardCopyOption.REPLACE_EXISTING); // StandardCopyOption.ATOMIC_MOVE throws Exception in thread "main" java.lang.UnsupportedOperationException: Unsupported copy option
							
							// Previous works
							// byte buf[] = new byte[64 * 1024 * 1024];
							// int len = 0;
							// while ((len = fis.read(buf, 0, 64 * 1024 * 1024)) > 0)
							//  	fos.write(buf, 0, len);
							// fos.close();
							// fis.close();
							
							// delete storage page file storageFilePath	
							// Previous works deleteFile(storageFilePath);
						}
						*/
						
						// copy existing page file from storage dir to local dir
						String storageFilePath = this.getFileNameByIndex(index);
						String localFilePath = this.getLocalFileNameByIndex(index);
						
						// just to be safe delete if a local file exists
						// because BigArray.size() uses acquirePage() which will read from local file if one already exists
						Path localPath = Paths.get(localFilePath);
						Files.deleteIfExists(localPath);
						
						// Storage handler
						// try {
							m_storageHandler.copyIfExistsToLocal(storageFilePath, localFilePath);
						//} catch (Exception e) {
						//	throw e;
						//}
						// End Storage handler
						// End New code	
						
						RandomAccessFile raf = null;
						FileChannel channel = null;
						try {
							// Modified code
							String localPageFilePath = this.getLocalFileNameByIndex(index); 
							raf = new RandomAccessFile(localPageFilePath, "rw");
							channel = raf.getChannel();
							MappedByteBuffer mbb = channel.map(READ_WRITE, 0, this.pageSize);
							// New code
							String storagePageFilePath = this.getFileNameByIndex(index);
							// End new code
							mpi = new MappedPageImpl(mbb, localPageFilePath, index, storagePageFilePath, m_storageHandler);
							cache.put(index, mpi, ttl, m_unfilledPage);
							if (logger.isDebugEnabled()) {
								logger.debug("Mapped page for " + localPageFilePath + " was just created and cached.");
							}
						} finally {
							if (channel != null) channel.close();
							if (raf != null) raf.close();
						}
					}
				}
			} finally {
				synchronized(mapLock) {
					pageCreationLockMap.remove(index);
				}
			}
	    } else {
	    	if (logger.isDebugEnabled()) {
	    		logger.debug("Hit mapped page " + mpi.getPageFile() + " in cache.");
	    	}
	    }
	
		return mpi;
	}

	// New method
	public IMappedPage acquirePageCreateOrModifyPage(long index) throws Exception {
		MappedPageImpl mpi = cache.get(index);
		if (mpi == null) { // not in cache, need to create one
			try {
				Object lock = null;
				synchronized(mapLock) {
					if (!pageCreationLockMap.containsKey(index)) {
						pageCreationLockMap.put(index, new Object());
					}
					lock = pageCreationLockMap.get(index);
				}
				synchronized(lock) { // only lock the creation of page index
					mpi = cache.get(index); // double check
					if (mpi == null) {
						
						// New code
						// Previous flushFiles() on BigArray would have deleted local array folder
						// and local page folders (index, data, meta-data)
						// so need to recreate local array folder and local page folder in m_localPageDirPath dir path
						// thats why .mkdir() is used here
						if (!m_localArrayDirPathFileObj.exists()) {
							boolean status = m_localArrayDirPathFileObj.mkdir();
							if (false == status)
								throw new IOException("Failed to create local array dir : " + m_localArrayDirPath + " Check if local parent directories exist");
						}
						
						if (!m_localPageDirPathFileObj.exists()) {
							boolean status = m_localPageDirPathFileObj.mkdir();
							if (false == status)
								throw new IOException("Failed to create local page dir : " + m_localPageDirPath + " Check if local parent directories exist");			
						}
					
						// copy existing page file from storage dir to local dir
						String storageFilePath = this.getFileNameByIndex(index);
						String localFilePath = this.getLocalFileNameByIndex(index);
						
						// just to be safe delete if a local file exists
						// because BigArray.size() uses acquirePage() which will read from local file if one already exists
						Path localPath = Paths.get(localFilePath);
						Files.deleteIfExists(localPath);
						
						// Storage handler
						m_storageHandler.copyIfExistsToLocal(storageFilePath, localFilePath);
						// End Storage handler
						// End New code	
						
						RandomAccessFile raf = null;
						FileChannel channel = null;
						try {
							// Modified code
							String localPageFilePath = this.getLocalFileNameByIndex(index); 
							raf = new RandomAccessFile(localPageFilePath, "rw");
							channel = raf.getChannel();
							MappedByteBuffer mbb = channel.map(READ_WRITE, 0, this.pageSize);
							// New code
							String storagePageFilePath = this.getFileNameByIndex(index);
							// End new code
							mpi = new MappedPageImpl(mbb, localPageFilePath, index, storagePageFilePath, m_storageHandler);
							m_unfilledPage = mpi;
							cache.put(index, mpi, ttl, m_unfilledPage);
							if (logger.isDebugEnabled()) {
								logger.debug("Mapped page for " + localPageFilePath + " was just created and cached.");
							}
						} finally {
							if (channel != null) channel.close();
							if (raf != null) raf.close();
						}
					}
				}
			} finally {
				synchronized(mapLock) {
					pageCreationLockMap.remove(index);
				}
			}
	    } else {
	    	if (logger.isDebugEnabled()) {
	    		logger.debug("Hit mapped page " + mpi.getPageFile() + " in cache.");
	    	}
	    }
	
		return mpi;
	}
	
	// New method
	private String getLocalFileNameByIndex(long index) {
		return m_localPageFilePath + index + PAGE_FILE_SUFFIX;
	}
	
	private String getFileNameByIndex(long index) {
		return this.pageFile + index + PAGE_FILE_SUFFIX;
	}


	public int getPageSize() {
		return pageSize;
	}

	public String getPageDir() {
		return pageDir;
	}

	public void releasePage(long index) {
		cache.release(index);
	}

	/**
	 * thread unsafe, caller need synchronization
	 * @throws Exception 
	 */
	@Override
	public void releaseCachedPages() throws IOException {
		cache.removeAll();
	}

	/**
	 * thread unsafe, caller need synchronization
	 */
	@Override
	public void deleteAllPages() throws IOException {
		cache.removeAll();
		Set<Long> indexSet = getExistingBackFileIndexSet();
		this.deletePages(indexSet);
		if (logger.isDebugEnabled()) {
			logger.debug("All page files in dir " + this.pageDir + " have been deleted.");
		}
		
		// Don't use this method deleteAllPages() because getExistingBackFileIndexSet() calls .listFiles() but we don't
		// have .listFiles() method in IStorageHandler
		// New code
		// try {
		// 	DMStdStorageHandler.deleteDirIfExists(this.pageDir);
		// } catch (Exception e) {
		//	throw new IOException(e);
		// }
		// End new code
	}
	
	/**
	 * thread unsafe, caller need synchronization
	 */
	@Override
	public void deletePages(Set<Long> indexes) throws IOException {
		if (indexes == null) return;
		for(long index : indexes) {
			this.deletePage(index);
		}
	}
	
	/**
	 * thread unsafe, caller need synchronization
	 */
	@Override
	public void deletePage(long index) throws IOException {
		// remove the page from cache first
		cache.remove(index);
		String fileName = this.getFileNameByIndex(index);
		int count = 0;
		int maxRound = 10;
		boolean deleted = false;
		while(count < maxRound) {
			try {
				FileUtil.deleteFile(new File(fileName));
				deleted = true;
				break;
			} catch (IllegalStateException ex) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
				}
				count++;
				if (logger.isDebugEnabled()) {
					logger.warn("fail to delete file " + fileName + ", tried round = " + count);
				}
			}
		}
		if (deleted) {
			logger.info("Page file " + fileName + " was just deleted.");
		} else {
			logger.warn("fail to delete file " + fileName + " after max " + maxRound + " rounds of try, you may delete it manually.");
		}
	}

	@Override
	public Set<Long> getPageIndexSetBefore(long timestamp) {
		Set<Long> beforeIndexSet = new HashSet<Long>();
		File[] pageFiles = this.pageDirFile.listFiles();
		if (pageFiles != null && pageFiles.length > 0) {
			for(File pageFile : pageFiles) {
				if (pageFile.lastModified() < timestamp) {
					String fileName = pageFile.getName();
					if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
						long index = this.getIndexByFileName(fileName);
						beforeIndexSet.add(index);
					}
				}
			}
		}
		return beforeIndexSet;
	}
	
	private long getIndexByFileName(String fileName) {
		int beginIndex = fileName.lastIndexOf('-');
		beginIndex += 1;
		int endIndex = fileName.lastIndexOf(PAGE_FILE_SUFFIX);
		String sIndex = fileName.substring(beginIndex, endIndex);
		long index = Long.parseLong(sIndex);
		return index;
	}

	/**
	 * thread unsafe, caller need synchronization
	 * @throws IOException 
	 */
	@Override
	public void deletePagesBefore(long timestamp) throws IOException {
		Set<Long> indexSet = this.getPageIndexSetBefore(timestamp);
		this.deletePages(indexSet);
		if (logger.isDebugEnabled()) {
			logger.debug("All page files in dir [" + this.pageDir + "], before [" + timestamp + "] have been deleted.");
		}
	}

	@Override
	public Set<Long> getExistingBackFileIndexSet() {
		Set<Long> indexSet = new HashSet<Long>();
		File[] pageFiles = this.pageDirFile.listFiles();
		if (pageFiles != null && pageFiles.length > 0) {
			for(File pageFile : pageFiles) {
				String fileName = pageFile.getName();
				if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
					long index = this.getIndexByFileName(fileName);
					indexSet.add(index);
				}
			}
		}
		return indexSet;
	}

	@Override
	public int getCacheSize() {
		return cache.size();
	}
	
	// for testing
	int getLockMapSize() {
		return this.pageCreationLockMap.size();
	}

	@Override
	public long getPageFileLastModifiedTime(long index) {
		String pageFileName = this.getFileNameByIndex(index);
		File pageFile = new File(pageFileName);
		if (!pageFile.exists()) {
			return -1L;
		}
		return pageFile.lastModified();
	}

	@Override
	public long getFirstPageIndexBefore(long timestamp) {
		Set<Long> beforeIndexSet = getPageIndexSetBefore(timestamp);
		if (beforeIndexSet.size() == 0) return -1L;
		TreeSet<Long> sortedIndexSet = new TreeSet<Long>(beforeIndexSet);
		Long largestIndex = sortedIndexSet.last();
		if (largestIndex != Long.MAX_VALUE) { // no wrap, just return the largest
			return largestIndex;
		} else { // wrapped case
			Long next = 0L;
			while(sortedIndexSet.contains(next)) {
				next++;
			}
			if (next == 0L) {
				return Long.MAX_VALUE;
			} else {
				return --next;
			}
		}
	}

	/**
	 * thread unsafe, caller need synchronization
	 */
	@Override
	public void flush() {
		Collection<MappedPageImpl> cachedPages = cache.getValues();
		for(IMappedPage mappedPage : cachedPages) {
			mappedPage.flush();
		}
	}

	@Override
	public Set<String> getBackPageFileSet() {
		Set<String> fileSet = new HashSet<String>();
		File[] pageFiles = this.pageDirFile.listFiles();
		if (pageFiles != null && pageFiles.length > 0) {
			for(File pageFile : pageFiles) {
				String fileName = pageFile.getName();
				if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
					fileSet.add(fileName);
				}
			}
		}
		return fileSet;
	}

	@Override
	public long getBackPageFileSize() {
		long totalSize = 0L;
		File[] pageFiles = this.pageDirFile.listFiles();
		if (pageFiles != null && pageFiles.length > 0) {
			for(File pageFile : pageFiles) {
				String fileName = pageFile.getName();
				if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
					totalSize += pageFile.length();
				}
			}
		}
		return totalSize;
	}

}
