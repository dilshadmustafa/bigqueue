package com.leansoft.bigqueue.page;

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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.log4j.Logger;

import com.dilmus.dilshad.storage.IStorageHandler;

public class MappedPageImpl implements IMappedPage, Closeable {
	
	private final static Logger logger = Logger.getLogger(MappedPageImpl.class);
	
	private ThreadLocalByteBuffer threadLocalBuffer;
	private volatile boolean dirty = false;
	private volatile boolean closed = false;
	private String pageFile;
	private long index;
	
	// New fields
	String m_storagePageFilePath;
	IStorageHandler m_storageHandler;
	
	public MappedPageImpl(MappedByteBuffer mbb, String pageFile, long index, String storagePageFilePath, IStorageHandler storageHandler) {
		this.threadLocalBuffer = new ThreadLocalByteBuffer(mbb);
		this.pageFile = pageFile;
		this.index = index;
		
		// New code
		m_storagePageFilePath = storagePageFilePath;
		m_storageHandler = storageHandler;
		// End new code
	}
	
	public void close() throws IOException {
		synchronized(this) {
			if (closed) return;
			
			// New code
			boolean moveFile = this.dirty;
			// End code
			
			flush();
			
			MappedByteBuffer srcBuf = (MappedByteBuffer)threadLocalBuffer.getSourceBuffer();
			unmap(srcBuf);
			
			/* Current works
			// New code
			Path localPath = Paths.get(this.pageFile);
			if (moveFile) {
				Path storagePath = Paths.get(m_storagePageFile);
				// if storage file of index exists then delete storage file
				Files.deleteIfExists(storagePath);
				// copy _local file to storage file
				Files.copy(localPath, storagePath, StandardCopyOption.REPLACE_EXISTING); // StandardCopyOption.ATOMIC_MOVE throws exception
			}
			// delete the _local here whether its dirty or not doesn't matter
			// otherwise lot of _local will pile up in local dir
			Files.delete(localPath);
			// End New code
			*/
			
			// New code Storage handler
			if (moveFile) {
				try {
					// if storage file of index exists then delete storage file
					// Not needed as of 02-Feb-2017 m_storageHandler.deleteIfExists(m_storagePageFilePath);
					// copy _local file to storage file
					m_storageHandler.copyFromLocal(m_storagePageFilePath, this.pageFile);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			// delete the _local here whether its dirty or not doesn't matter
			// otherwise lot of _local will pile up in local dir
			Path localPath = Paths.get(this.pageFile);
			Files.delete(localPath);
			// End New code Storage handler
			
			this.threadLocalBuffer = null; // hint GC
			
			closed = true;
			if (logger.isDebugEnabled()) {
				logger.debug("Mapped page for " + this.pageFile + " was just unmapped and closed.");
			}
		}
	}
	
	// New method
	public boolean isDirty() {
		return this.dirty;
	}
	
	@Override
	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}
	
	@Override
	public void flush() {
		synchronized(this) {
			if (closed) return;
			if (dirty) {
				MappedByteBuffer srcBuf = (MappedByteBuffer)threadLocalBuffer.getSourceBuffer();
				srcBuf.force(); // flush the changes
				dirty = false;
			
				if (logger.isDebugEnabled()) {
					logger.debug("Mapped page for " + this.pageFile + " was just flushed.");
				}
			}
		}
	}

	public byte[] getLocal(int position, int length) {
		ByteBuffer buf = this.getLocal(position);
		byte[] data = new byte[length];
		buf.get(data);
		return data;
	}
	
	@Override
	public ByteBuffer getLocal(int position) {
		ByteBuffer buf = this.threadLocalBuffer.get();
		buf.position(position);
		return buf;
	}
	
	private static void unmap(MappedByteBuffer buffer)
	{
		Cleaner.clean(buffer);
	}
	
    /**
     * Helper class allowing to clean direct buffers.
     */
    private static class Cleaner {
        public static final boolean CLEAN_SUPPORTED;
        private static final Method directBufferCleaner;
        private static final Method directBufferCleanerClean;

        static {
            Method directBufferCleanerX = null;
            Method directBufferCleanerCleanX = null;
            boolean v;
            try {
                directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
                directBufferCleanerX.setAccessible(true);
                directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
                directBufferCleanerCleanX.setAccessible(true);
                v = true;
            } catch (Exception e) {
                v = false;
            }
            CLEAN_SUPPORTED = v;
            directBufferCleaner = directBufferCleanerX;
            directBufferCleanerClean = directBufferCleanerCleanX;
        }

        public static void clean(ByteBuffer buffer) {
    		if (buffer == null) return;
            if (CLEAN_SUPPORTED && buffer.isDirect()) {
                try {
                    Object cleaner = directBufferCleaner.invoke(buffer);
                    directBufferCleanerClean.invoke(cleaner);
                } catch (Exception e) {
                    // silently ignore exception
                }
            }
        }
    }
    
    private static class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
    	private ByteBuffer _src;
    	
    	public ThreadLocalByteBuffer(ByteBuffer src) {
    		_src = src;
    	}
    	
    	public ByteBuffer getSourceBuffer() {
    		return _src;
    	}
    	
    	@Override
    	protected synchronized ByteBuffer initialValue() {
    		ByteBuffer dup = _src.duplicate();
    		return dup;
    	}
    }

	@Override
	public boolean isClosed() {
		return closed;
	}
	
	public String toString() {
		return "Mapped page for " + this.pageFile + ", index = " + this.index + ".";
	}

	@Override
	public String getPageFile() {
		return this.pageFile;
	}

	@Override
	public long getPageIndex() {
		return this.index;
	}
}
