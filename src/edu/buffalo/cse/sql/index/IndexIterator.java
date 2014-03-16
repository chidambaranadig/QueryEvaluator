
package edu.buffalo.cse.sql.index;

import java.util.Iterator;

import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;

public interface IndexIterator extends Iterator<Datum[]> {
  public void close() throws SqlException;
}