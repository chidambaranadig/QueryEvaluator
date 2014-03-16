
package edu.buffalo.cse.sql.index;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.ManagedFile;

public class DatumStreamIterator implements IndexIterator {
  ManagedFile file;
  int currPage;
  int currRecord;
  int maxPage;
  Datum[] maxRecord;
  DatumBuffer currBuffer;
  Datum[] rowBuffer;
  IndexKeySpec key;
  Schema.Type[] schema;
  
  public DatumStreamIterator(ManagedFile file, Schema.Type[] schema)
  {
    this.file = file;
    this.schema = schema;
    this.key = null;
    currPage = 0;
    maxPage = (int)(file.size() / BufferManager.pageSize);
    currRecord = 0;
    maxRecord = null;
    currBuffer = null;
    rowBuffer = null;
  }
  
  public DatumStreamIterator currPage(int currPage)
    { this.currPage = currPage; return this; }
  
  public DatumStreamIterator maxPage(int maxPage)
    { this.maxPage = maxPage; return this; }

  public DatumStreamIterator currRecord(int currRecord)
    { this.currRecord = currRecord; return this; }
  
  public DatumStreamIterator maxRecord(Datum[] maxRecord)
    { this.maxRecord = maxRecord; return this; }
  
  public DatumStreamIterator key(IndexKeySpec key)
    { this.key = key; return this; }
  
  public DatumStreamIterator ready()
    throws BufferException, IOException
  {
    advancePage();
    return this;
  }
  
  protected boolean advancePage()
    throws BufferException, IOException
  {
    if(currBuffer != null){
      file.unpin(currPage);
      currPage++;
      currRecord = 0;
    }
    if(currPage > maxPage) return false;
    file.pin(currPage);
    currBuffer = new DatumBuffer(file.getBuffer(currPage), schema);
    System.out.println("Loading page "+currPage+"/"+maxPage+" with "+currBuffer.length()+" records");
    return true;
  }
  
  public boolean buffer()
  {
    try {
      if(rowBuffer == null){ 
        while(currRecord >= currBuffer.length())
          { if(!advancePage()) { return false; } }
        rowBuffer = currBuffer.read(currRecord);
        currRecord++;
      }
      if(maxRecord != null){
        if(key != null){
          System.out.println(Datum.stringOfRow(key.createKey(rowBuffer))+ "<==>"+ Datum.stringOfRow(maxRecord));
          return (key.compare(key.createKey(rowBuffer), maxRecord) < 0);
        } else {
          return Datum.compareRows(rowBuffer, maxRecord) <= 0;
        }
      } else { return true; }
    } catch(Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    return false;
  }
  
  public boolean hasNext()
  {
    return buffer();
  }
  
  public Datum[] next()
  {
    Datum[] ret = null;
    if(buffer()){ 
      ret = rowBuffer;
      rowBuffer = null;
    }
    return ret;
  }
  
  public void close()
    throws SqlException
  {
    try {
      if(currPage <= maxPage){
        file.unpin(currPage);
      }
      currBuffer = null;
    } catch(Exception e){ throw new SqlException("Error while closing", e); }
  }

  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}