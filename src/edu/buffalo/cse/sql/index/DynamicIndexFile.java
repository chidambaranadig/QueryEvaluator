/**
 * An extension of IndexFile that also supports insertion and deletions
 **/

package edu.buffalo.cse.sql.index;

import java.io.IOException;
import java.util.Iterator;

import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;

public interface DynamicIndexFile extends IndexFile {
  
  public void addRow(Datum[] row) 
    throws SqlException, IOException;

  public void deleteRow(Datum[] row) 
    throws SqlException, IOException;  

}