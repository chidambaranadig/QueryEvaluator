/**
 * Exception thrown by DatumBuffer when a DatumBuffer is full and can no longer
 * be written to.
 **/
 
package edu.buffalo.cse.sql.data;

import edu.buffalo.cse.sql.SqlException;

public class InsufficientSpaceException extends SqlException {
  public InsufficientSpaceException() { super("Insufficient Space"); }
}