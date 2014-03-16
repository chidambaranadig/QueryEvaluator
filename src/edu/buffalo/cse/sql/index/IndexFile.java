/**
 * The interface for a generic index.
 * 
 * scan() should return an iterator over all data in the index
 * 
 * rangeScan[To|From]() should return an iterator over the specified range, or
 * throw a SqlException if range scans are not supported
 * 
 * get() should return the datum for the corresponding key
 **/

package edu.buffalo.cse.sql.index;

import java.io.IOException;
import java.util.Iterator;

import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;

public interface IndexFile {
  
  public IndexIterator scan() 
    throws SqlException, IOException;

  public IndexIterator rangeScanTo(Datum[] toKey)
    throws SqlException, IOException;

  public IndexIterator rangeScanFrom(Datum[] fromKey)
    throws SqlException, IOException;

  public IndexIterator rangeScan(Datum[] start, Datum[] end)
    throws SqlException, IOException;

  public Datum[] get(Datum[] key)
    throws SqlException, IOException;
  
}