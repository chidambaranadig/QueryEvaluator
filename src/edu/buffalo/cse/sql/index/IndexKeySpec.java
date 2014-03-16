
package edu.buffalo.cse.sql.index;

import java.util.Comparator;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.Schema;

public interface IndexKeySpec extends Comparator<Datum[]> {

  public Datum[] createKey(Datum[] row);
  public int hashRow(Datum[] row);

  public int hashKey(Datum[] key);

  public Schema.Type[] rowSchema();
  public Schema.Type[] keySchema();
}