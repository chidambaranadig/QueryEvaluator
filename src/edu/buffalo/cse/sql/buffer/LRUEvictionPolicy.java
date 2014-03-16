/**
 * An LRU (Least-recently-used) eviction policy for the BufferManager.  
 * 
 * Keeps a queue of pages to be evicted.  Touching a page moves it to the back
 * of the queue.  When eviction time comes, the head of the queue is evicted.
 **/
package edu.buffalo.cse.sql.buffer;

import java.util.LinkedHashSet;

public class LRUEvictionPolicy implements EvictionPolicy {
  //LinkedHashSet guarantees that iteration order is equivalent to insertion
  //order (insertions of elements already in the set don't count).  This makes
  //it ideal for an LRU cache (touching == removing and reinserting the page).
  LinkedHashSet<Integer> evictionOrder;
  
  public LRUEvictionPolicy()
  {
    this.evictionOrder = new LinkedHashSet<Integer>();
  }
    
  public void add(int page)
  {
    evictionOrder.add(page);
  }
  public void remove(int page)
  {
    evictionOrder.remove(page);
  }
  public void touch(int page)
  {
    remove(page); add(page);
  }
  public int selectCandidate()
  {
    if(evictionOrder.size() == 0){ return -1; }
    return evictionOrder.iterator().next();
  }
  
}

