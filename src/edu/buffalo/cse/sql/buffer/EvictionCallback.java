/**
 * Interface used by the BufferManager to signal when a frame is about to be
 * evicted.  The implementor should perform any local cleanup, and flush any
 * dirty data in the page to disk.
 **/
package edu.buffalo.cse.sql.buffer;

public interface EvictionCallback {
  
  public void evict(int frame) throws EvictionFailure;
  
}