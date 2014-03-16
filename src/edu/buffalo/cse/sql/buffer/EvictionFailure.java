/**
 * Exception used to signal that something has gone horribly wrong with an 
 * eviction.  This is usually meant to be a non-recoverable error.
 **/
package edu.buffalo.cse.sql.buffer;

public class EvictionFailure extends BufferException {
  public EvictionFailure(Throwable error) { super("Eviction Failure", error); }
}