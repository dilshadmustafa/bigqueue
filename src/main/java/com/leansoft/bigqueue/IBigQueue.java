package com.leansoft.bigqueue;

import java.io.Closeable;
import java.io.IOException;

/**
 * Queue ADT
 * 
 * @author bulldog
 *
 */
public interface IBigQueue extends Closeable {

	/**
	 * Determines whether a queue is empty
	 * 
	 * @return ture if empty, false otherwise
	 */
	public boolean isEmpty();
	
	/**
	 * Adds an item at the back of a queue
	 * 
	 * @param data to be enqueued data
	 * @throws IOException exception throws if there is any IO error during enqueue operation.
	 */
	public void enqueue(byte[] data)  throws Exception;
	
	/**
	 * Retrieves and removes the front of a queue
	 * 
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during dequeue operation.
	 */
	public byte[] dequeue() throws Exception;
	
	/**
	 * Removes all items of a queue, this will empty the queue and delete all back data files.
	 * 
	 * @throws IOException exception throws if there is any IO error during dequeue operation.
	 */
	public void removeAll() throws Exception;
	
	/**
	 * Retrieves the item at the front of a queue
	 * 
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during peek operation.
	 */
	public byte[] peek()  throws Exception;
	
	/**
	 * Delete all used data files to free disk space.
	 * 
	 * BigQueue will persist enqueued data in disk files, these data files will remain even after
	 * the data in them has been dequeued later, so your application is responsible to periodically call
	 * this method to delete all used data files and free disk space.
	 * 
	 * @throws IOException exception throws if there is any IO error during gc operation.
	 */
	public void gc() throws Exception;
	
	/**
	 * Force to persist current state of the queue, 
	 * 
	 * normally, you don't need to flush explicitly since:
	 * 1.) BigQueue will automatically flush a cached page when it is replaced out,
	 * 2.) BigQueue uses memory mapped file technology internally, and the OS will flush the changes even your process crashes,
	 * 
	 * call this periodically only if you need transactional reliability and you are aware of the cost to performance.
	 */
	public void flush();
	
	/**
	 * Total number of items available in the queue.
	 * @return total number
	 */
	public long size();
}
