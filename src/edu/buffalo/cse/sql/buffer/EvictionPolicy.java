/**
 * Interface used to define eviction policies for the BufferManager.  
 * 
 * add(page) should enter [page] into consideration for replacement.
 * remove(page) should remove [page] from consideration for replacement.
 * touch(page) indicates that [page] has received recent activity.
 *
 * selectCandidate() should return a page being considered for replacement and
 *                   subsequently remove it from future consideration.
 **/

package edu.buffalo.cse.sql.buffer;

public interface EvictionPolicy {
  
  public void add(int page);
  public void remove(int page);
  public void touch(int page);
  
  //Select a page for eviction, but don't actually evict it.
  public int selectCandidate();
  
}