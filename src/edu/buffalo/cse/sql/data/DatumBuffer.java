/**
 * Utility class for serializing datum rows (Datum[]) into a ByteBuffer
 * 
 * A DatumBuffer wraps around a ByteBuffer, and provides a set of utility 
 * methods for writing and reading rows from the ByteBuffer.
 * 
 * A DatumBuffer lays out data as follows
 * Bytes 0 to ([reserved]-1): Bytes reserved for caller-specific metadata
 * Bytes [reserved] to ([reserved]+N): Densely packed, arbitrary-length records
 * Bytes [reserved]+N to (([length]+2)*4): Free Space
 * Bytes ([size] - ([length]+2)*4) to ([size] - ([length]+1)*4 - 1): 
 *                              Integer pointer to the first byte of free space
 * Bytes ([size] - ([length]+1)*4) to ([size] - 4 - 1): 
 *                              Integer pointers to each record in the buffer
 * Bytes ([size] - 4) to ([size]-1): Integer encoding [length]
 * 
 *
 * INITIALIZATION: 
 * When a ByteBuffer is first used as a DatumBuffer, it is REQUIRED that you 
 * initialize() it.  This sets the buffer to be completely empty (no rows), and 
 * will delete any row already present in the buffer -- you only need to do this
 * once.  
 *
 *    Initializing a DatumBuffer: 
 *      new DatumBuffer(byteBuffer, null).initialize()
 * 
 * During initialization you MAY request that some space in the buffer be
 * reserved for your own uses.  This can be useful if you need to store page
 * pointers (e.g., for a linked list of pages) in the ByteBuffer.  The reserved
 * space occupies the first N bytes, where N is the number of bytes reserved.
 *
 * USAGE: 
 *    Wrapping a ByteBuffer with a DatumBuffer:
 *      new DatumBuffer(byteBuffer, rowSchema)
 * 
 * DatumBuffer provides two utility methods: read() and write()
 * 
 * write(row) appends a record (row) to the ByteBuffer and updates metadata 
 *            appropriately.  Write returns the recordID of the new record.  If 
 *            the new record will not fit in the ByteBuffer, write throws an 
 *            InsufficientSpaceException.
 * 
 * read(id)   reads a record with the specified recordID (id) from the 
 *            ByteBuffer.  The record is assumed to follow the schema provided
 *            when the DatumBuffer was initialized.  
 * 
 * Note that only read uses rowSchema.  If you are writing only, you may 
 * initialize DatumBuffer with a null rowSchema.
 **/
package edu.buffalo.cse.sql.data;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.buffer.BufferManager;

public class DatumBuffer {
  ByteBuffer buffer;
  Schema.Type[] schema;

  public DatumBuffer(ByteBuffer buffer, Schema.Type[] schema)
  {
    this.buffer = buffer;
    this.schema = schema;
  }
  
  protected int write(int position, Datum d)
    { return DatumSerialization.write(buffer, position, d); }
  
  protected Datum read(int position, Schema.Type t) throws UnsupportedEncodingException
    { return DatumSerialization.read(buffer, position, t); }
  
  public void initialize()
  {
    initialize(0);
  }
  public void initialize(int reservedSpace)
  {
    setAddr(-1, 0);
    setAddr(0, reservedSpace);
  }
  
  public int write(Datum[] row)
    throws InsufficientSpaceException
  {
    if(DatumSerialization.getLength(row) > remaining()-8) 
      { throw new InsufficientSpaceException(); }
    int start = freeAddr();
//    System.out.println("Starting row write @ byte "+start);
    int i = DatumSerialization.write(buffer, start, row);
    
    int idx = length();
    
    setAddr(-1, length()+1);
    setAddr(length(), start + i);
//    System.out.println("Used "+(i+4)+" bytes; now remaining "+remaining()+" bytes");

    return idx;
  }
  
  public Datum[] read(int idx) throws UnsupportedEncodingException
  {
    return DatumSerialization.read(buffer, addr(idx), schema);
  }
  
  public int length()
  {
    return addr(-1);
  }
  
  protected int binarySearch(int start, int length, Comparable<Datum[]> target) throws UnsupportedEncodingException
  {
    if(length <= 0) { return start; }
    int sep = length / 2;
    Datum[] mid = read(start + sep);
    int offset = target.compareTo(mid);
    if(offset == 0){ return start+sep; }
    if(offset < 0){ return binarySearch(start, length-sep-1, target); }
    if(length == 1){
      mid = read(start+1);
      offset = target.compareTo(mid);
      if(offset >= 0){ return start+1; }
      return start;
    }
    return binarySearch(start+sep+1, length-sep-1, target);
  }
  
  public int find(Comparable<Datum[]> target) throws UnsupportedEncodingException
    { return binarySearch(0, length(), target); }

  public int find(final Datum[] target) throws UnsupportedEncodingException
  {
    return find(new Comparable<Datum[]>() {
        public int compareTo(Datum[] x) 
          { return Datum.compareRows(target, x); }
      });
  }
  
  protected int freeAddr()
  {
    return addr(length());
  }
  
  protected int addr(int i)
  {
    return buffer.getInt(BufferManager.pageSize - (4 * (i+2)));
  }
  
  protected void setAddr(int i, int v)
  {
    buffer.putInt(BufferManager.pageSize - (4 * (i+2)), v);
  }
  
  public int remaining()
  {
    return BufferManager.pageSize - freeAddr() - (length() + 1) * 4;
  }
}