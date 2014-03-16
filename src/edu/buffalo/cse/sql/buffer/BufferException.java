/**
 * An error that occurs in the buffer manager
 **/

package edu.buffalo.cse.sql.buffer;

import edu.buffalo.cse.sql.SqlException;

public class BufferException extends SqlException {
  public BufferException(String msg) { super(msg); }
  public BufferException(String msg, Throwable cause) { super(msg, cause); }
}