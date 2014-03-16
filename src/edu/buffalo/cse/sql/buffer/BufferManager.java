/**
 * Paging infrastructure akin to a buffer manager.  The BufferManager maintains
 * a pool of N frames, each enough to store a single page of [pageSize] bytes. 
 * 
 * Each frame is represented as a single instance of java.nio.ByteBuffer.  This
 * handy utility class provides not only a fixed size memory space, but also
 * tools for encoding/decoding primitive values.
 * 
 * The buffer manager API supports 5 basic accessors.
 * 
 * allocate: Attempt to obtain a buffer frame.  If a frame is free, it will be 
 *           immediately allocated.  If a frame is not free, allocate will 
 *           select a frame for eviction according to its eviction policy and 
 *           then block until the frame has been flushed to disk by its previous 
 *           resident.  Returns the frameID of the allocated page.  Use get
 *           to obtain the actual ByteBuffer.
 *
 *           When allocating frame, an eviction callback must be included.  
 *           This callback will be used to signal when the page is about to be
 *           released, and the caller should immediately flush the page to disk.
 * 
 * free:     Indicate that a page is no longer of interest.  Freeing pages is 
 *           not required, as out-of-use page will eventually be evicted 
 *           eventually, regardless.
 * 
 * get:      Obtain the ByteBuffer corresponding to a specific frameID.
 * 
 * pin:      Pin an allocated buffer page.  The page will not be replaced as 
 *           long as the pin holds.  Each pin must have a corresponding unpin
 *           somewhere in the code.  Pins are cumulative.  Calling pin multiple
 *           times on the same page requires the same number of unpins to 
 *           release the pinned page.
 * 
 * unpin:    Unpin a pinned buffer page.  There must be one unpin for each pin.
 *
 **/

package edu.buffalo.cse.sql.buffer;

import java.nio.ByteBuffer;

public class BufferManager {
  
  public static final int pageSize = 1024;

  ByteBuffer[] bufferPool;
  EvictionCallback[] callbacks;
  int[] pinCount;
  int nextFreePage;
  EvictionPolicy evictionPolicy;
  
  public BufferManager(int pages)
  {
    this(pages, new LRUEvictionPolicy());
  }
  
  public BufferManager(int pages, EvictionPolicy policy)
  {
    bufferPool = new ByteBuffer[pages];
    pinCount = new int[pages];
    for(int i = 0; i < pages; i++){
      bufferPool[i] = ByteBuffer.allocate(pageSize);
      bufferPool[i].putInt(0, (i < (pages-1)) ? i+1 : -1);
      pinCount[i] = 0;
    }
    nextFreePage = 0;
    callbacks = new EvictionCallback[pages];
    this.evictionPolicy = policy;
  }
  
  public ByteBuffer get(int i)
  {
    return bufferPool[i];
  }
  
  public void pin(int page)
    throws BufferException
  {
//    System.out.println("Pin " + page);
    if(pinCount[page] == 0){
      evictionPolicy.remove(page);
    }
    pinCount[page] ++;
  }
  
  public void unpin(int page)
    throws BufferException
  {
//    System.out.println("Unpin " + page);
    if(pinCount[page] <= 0){
      throw new BufferException("Attempt to unpin an unpinned page");
    }
    pinCount[page] -= 1;
    if(pinCount[page] == 0){
//      System.out.println("Page is free " + page);
      evictionPolicy.add(page);
    }
  }
  
  public void touch(int page)
  {
    if(pinCount[page] <= 0){ evictionPolicy.touch(page); }
  }
  
  public void free(int page)
    throws BufferException
  {
    if(pinCount[page] > 0) {
      throw new BufferException("Attempt to free a pinned page");
    }
    evictionPolicy.remove(page);
    bufferPool[page].putInt(0, nextFreePage);
    nextFreePage = page;
    callbacks[page] = null;
  }
  
  private int evictAndTransfer(EvictionCallback callback)
    throws BufferException
  {
    //doesn't actually evict it, just selects
    int page = evictionPolicy.selectCandidate();
    if(page < 0){
      throw new AllocationException("Insufficient unpinned pages in buffer pool");
    }
    if(pinCount[page] > 0) {
      throw new BufferException("Bug: Trying to free a pinned page");
    }
    if(callbacks[page] != null){
      callbacks[page].evict(page);
    }
    callbacks[page] = callback;
    evictionPolicy.touch(page);
    pinCount[page] = 0;
    return page;
  }
  
  public int allocate(EvictionCallback callback)
    throws BufferException
  {
    if(nextFreePage < 0){
      return evictAndTransfer(callback);
    } else {
      int allocated = nextFreePage;
      nextFreePage = bufferPool[allocated].getInt(0);
      bufferPool[allocated].clear();
      callbacks[allocated] = callback;
      evictionPolicy.add(allocated);
      pinCount[allocated] = 0;
      return allocated;
    }
  }
}