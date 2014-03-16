/**
 * Interface on top of the buffer manager that provides access to data files.
 * 
 * Three methods are provided: 
 *
 * open() opens an existing file or returns a pointer to an existing instance
 *        of the same file.
 * openTemp() opens a temporary file of a specified size.
 * close() closes a fie that has been opened.
 **/

package edu.buffalo.cse.sql.buffer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class FileManager {
  
  BufferManager bufferManager;
  
  HashMap<File, ManagedFile> openFiles;
  
  public FileManager(BufferManager bufferManager)
  {
    this.bufferManager = bufferManager;
    openFiles = new HashMap<File, ManagedFile>();
  }
  
  public ManagedFile open(File file)
    throws IOException
  {
    ManagedFile ret = openFiles.get(file);
    if(ret != null){ ret.refcount += 1; return ret; }
    ret = new ManagedFile(file, bufferManager);
    openFiles.put(file, ret);
    return ret;
  }
  
  public ManagedFile openTemp(int size)
    throws IOException
  {
    ManagedFile ret = open(File.createTempFile("SQL", ".bufferfile"));
    ret.resize(size);
    return ret;
  }
  
  public void close(File file)
    throws IOException, BufferException
  {
    ManagedFile ret = openFiles.get(file);
    if(ret != null){ 
      ret.refcount -= 1; 
      if(ret.refcount <= 0){
        ret.flush();
        openFiles.remove(file);
      }
    }
  }
  
  protected void removeFile(File f){ openFiles.remove(f); }
}