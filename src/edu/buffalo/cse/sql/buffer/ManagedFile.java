/**
 * A collection of data pages backed by a file on disk.  
 * 
 * A ManagedFile provides a convenient frontend for interacting with the 
 * BufferManager.  The ManagedFile represents a file as a sequence of pages, 
 * each of which is represented, as in BufferManager, as a ByteBuffer.  
 * 
 * Unlike the BufferManager, which uses frameIDs to refer to memory pages, 
 * ManagedFile refers to pages by their index in the file.  ManagedFile exposes
 * a similar interface to the buffer manager (getPage(), pin(), unpin()), each
 * of which uses the pageID instead of the frameID.  If a page has not yet been
 * loaded into memory, the getPage() and pin() operations will load the page
 * in.
 *
 * ManagedFile also provides eviction protection.  If a dirty page is evicted
 * by the BufferManager, ManagedFile will ensure that the contents make it to
 * disk before the BufferManager finishes evicting the page.
 * 
 * resize(): Set the file size to the specified number of pages.  Pages in the
 *           overlap between the old size and the new size will be copied.
 *
 * getBuffer(p):     Return a ByteBuffer containing page [p].  If this page has 
 *                   already been loaded into memory, it is returned.  If not, 
 *                   the page is loaded into memory before getBuffer() returns.
 * 
 * safeGetBuffer(p): As getBuffer(), but first a check is performed to ensure 
 *                   that the file is big enough to contain [p].  If not, the
 *                   file size is progressively doubled until it it has a page 
 *                   [p].
 * 
 * dirty(p):         Indicate that page [p] is dirty.  When the file is 
 *                   flushed() or the page is evicted by the buffer manager, 
 *                   page [p] will be written to disk.
 *
 * pin(p):           As getBuffer(), but also pin the page being returned.  
 *
 * safePin(p):       As safeGetBuffer(), but also pin the page being returned.
 * 
 * unpin(p[, d]):    Unpin the indicated page.  If the optional dirty bit is
 *                   both present and true, also indicate that the page is 
 *                   dirty.
 *                   unpin(p, true); is equivalent to dirty(p); unpin(p);
 *                   unpin(p, false); is equivalent to just unpin(p);
 * 
 * flush():          Flush all dirty pages to disk, and block until the flush
 *                   is complete.
 *
 * ensureSize(s):    If the file is smaller than [s], resize it to [s] pages.
 *
 * ensureSizeByDoubling(s): If the file is smaller than [s], repeatedly double
 *                   the size of the file until it is bigger than [s] pages.  
 *                   This is an effective way to avoid repeated (expensive) 
 *                   resizing operations on a linearly growing file.
 * 
 **/
package edu.buffalo.cse.sql.buffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ManagedFile { 
  
  BufferManager manager;
  File file;
  FileChannel access;
  Page[] pages;
  int refcount = 1;
  
  protected ManagedFile(File file, BufferManager manager)
    throws IOException
  {
    access = new RandomAccessFile(file, "rw").getChannel();
    this.file = file;
    this.manager = manager;
    resizeBytes(access.size());
  }
  
  public void resizeBytes(long numBytes)
    throws IOException
  {
    resize((int)(numBytes / BufferManager.pageSize) + 
                     (int)((numBytes % BufferManager.pageSize > 0) ? 1 : 0));
  }
  
  public void resize(int newNumPages)
    throws IOException
  {
    Page[] oldPages = pages;
    if(newNumPages * BufferManager.pageSize < access.size()){ 
      access.truncate(newNumPages * BufferManager.pageSize); 
    }
//    System.out.println("Resizing to " + newNumPages);
    pages = new Page[newNumPages];
    int i = 0;
    if(oldPages != null) {
      for(i = 0; i < Math.min(pages.length, oldPages.length); i++){
        pages[i] = oldPages[i];
      }
    }
    for(; i < pages.length; i++){
      pages[i] = new Page(i);
    }
  //  access.force(true);
  }
  
  public ByteBuffer getBuffer(int page)
    throws BufferException, IOException
  {
    return pages[page].getBuffer();
  }
  
  public ByteBuffer safeGetBuffer(int page)
    throws BufferException, IOException
  {
    ensureSizeByDoubling(page+1);
    return getBuffer(page);
  }
  
  public void dirty(int page)
    throws BufferException
  {
    pages[page].dirty();
  }
  
  public ByteBuffer pin(int page)
    throws BufferException, IOException
  {
    return pages[page].pin();
  }
  
  public ByteBuffer safePin(int page)
    throws BufferException, IOException
  {
    ensureSizeByDoubling(page+1);
    return pin(page);
  }
  
  public void unpin(int page, boolean dirty)
    throws BufferException
  {
    if(dirty){ dirty(page); }
    unpin(page);
  }
  
  public void unpin(int page)
    throws BufferException
  {
    pages[page].unpin();
  }
  
  public void flush()
    throws BufferException, IOException
  {
    for(Page p : pages){ p.flush(); }
  }
  
  public int size()
  {
    return pages.length;
  }
  
  public void ensureSize(int desiredSize)
    throws IOException
  {
    if(size() < desiredSize) { 
      resize(desiredSize);
    }
  }
  public void ensureSizeByDoubling(int desiredSize)
    throws IOException
  {
    if(size() < desiredSize) { 
      int newSize = size();
      while(newSize < desiredSize){ newSize *= 2; }
      resize(newSize);
    }
  }

  protected class Page implements EvictionCallback {
    int id;
    int bufferPage;
    boolean dirty;
    
    public Page(int id)
    {
      this.id = id;
      bufferPage = -1;
    }
    
    public void setBuffer(int bufferPage){ this.bufferPage = bufferPage; }
    
    protected boolean isAllocated() { return bufferPage >= 0; }
    
    protected void allocate()
      throws BufferException
    {
      if(isAllocated()){ 
        throw new BufferException("Bug: allocating an already allocated page");
      }
      bufferPage = manager.allocate(this);
    }
    
    public ByteBuffer getBuffer()
      throws BufferException, IOException
    {
      ByteBuffer buffer;
      if(!isAllocated()){ 
        dirty = false;
        allocate();
//        System.out.println("Reading page " + id + " @ " + bufferPage);
        buffer = manager.get(bufferPage);
        buffer.position(0);
        int bytesRead = access.read(buffer, position());
//        if(bytesRead < BufferManager.pageSize) {
//          System.out.println("ERROR. Insufficient read data (Only read "+bytesRead+")");
//        }
      } else {
        buffer = manager.get(bufferPage);
      }
      manager.touch(bufferPage);
      return buffer;
    }
    
    public void evict(int page)
      throws EvictionFailure
    { 
      try { flush(); bufferPage = -1; } 
      catch(Exception e) { throw new EvictionFailure(e); }
    }
    
    public void flush()
      throws BufferException, IOException
    {
//      System.out.println("Flushing page " + id + " @ " + bufferPage);
      if(dirty && (bufferPage >= 0)){
        ByteBuffer buffer = manager.get(bufferPage);
        buffer.position(0);
        int bytesWritten = access.write(buffer, position());
//        if(bytesWritten < BufferManager.pageSize) {
//          System.out.println("ERROR. Insufficient written data (only "+bytesWritten+" bytes)");
//        }
      //  access.force(true);
        dirty = false;
      }
    }
    
    public void dirty()
      throws BufferException
    {
      dirty = true;
    }
    
    public ByteBuffer pin()
      throws BufferException, IOException
    {
      ByteBuffer buff = getBuffer();
      manager.pin(bufferPage);
      return buff;
    }
    
    public void unpin()
      throws BufferException
    {
      manager.unpin(bufferPage);
    }
    
    public long position()
    {
      long position = BufferManager.pageSize;
      position *= (long)id;
      return position;
    }
  }
}