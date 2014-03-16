/**
 * An error that occurs during page allocation in the buffer manager
 **/

package edu.buffalo.cse.sql.buffer;

public class AllocationException extends BufferException {
  public AllocationException(String msg) { super(msg); }
}