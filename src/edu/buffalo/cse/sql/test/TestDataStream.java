/**
 * A testing harness that generates arbitrary length data streams.  Thanks to a
 * hardcoded seed value, the data streams for one set of parameters (#keys, ...)
 * will be random, but identical each time.
 * 
 * You can vary the # of keys, the # of non-key values, and the the # of rows
 * generated.  Each data column will be an integer.  Keys are guaranteed to be
 * generated in monotonically increasing order, with keys on the left taking 
 * priority (in other words, the generated dataset is already sorted).
 * 
 * You can fine-tune the behavior of the key-incrementing process.  There is a
 * chaos parameter that controls how rapidly key-steps occur.  The higher this
 * value is, the more different adjacent keys will be.
 * 
 * The guaranteeKeyStep parameter, if true, will ensure that no two data rows
 * will have the same key.  If false, two data rows may have the same key -- 
 * especially for low values of the chaos parameter.
 *
 * TestDataStream also includes a pair of validation methods.  These allow you
 * to verify the accuracy of an index scan iterator that you provide.  Entries
 * in the provided iterator will be read off, invalid entries will be reported
 * as an error, and the method will return false
 **/

package edu.buffalo.cse.sql.test;

import java.util.Random;
import java.util.Iterator;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.data.Datum;

public class TestDataStream implements Iterator<Datum[]> {
  
  int rows;
  int values;
  int[] curr;
  int chaos;
  boolean guaranteeKeyStep;
  Random rand;
  
  public TestDataStream(int keys, int values, int rows)
  { 
    this(keys, values, rows, keys*10, true);
  }

  public TestDataStream(int keys, int values, int rows, int chaos, 
                        boolean guaranteeKeyStep)
  {
    this.rows = rows;
    this.values = values;
    this.curr = new int[keys];
    this.chaos = chaos;
    this.guaranteeKeyStep = guaranteeKeyStep; 
    for(int i = 0; i < this.curr.length; i++){ this.curr[i] = 0; }
    rand = new Random(52982);
  }
  
  public Schema.Type[] getSchema()
  {
    int cols = values + curr.length;
    Schema.Type[] sch = new Schema.Type[cols];
    for(int i = 0; i < cols; i++){ sch[i] = Schema.Type.INT; }
    return sch;
  }
  
  public boolean validate(Iterator<Datum[]> inStream)
  {
    int i = 0;
    while(hasNext()){
      if(!inStream.hasNext()) { 
        System.err.println("Provided stream ended early");
        return false; 
      }
      i++;
      Datum[] expected = next(), found = inStream.next();
      if(!Datum.rowEquals(expected, found)){ 
        System.err.println("At Row: " + i);
        System.err.println("Expected: "+Datum.stringOfRow(expected));
        System.err.println("Found: "+Datum.stringOfRow(found));
        return false; 
      }
    }
    if(inStream.hasNext()){
      System.err.println("Provided stream is too long");
      return false;
    }
    return true;
  }
  
  public boolean validate(Iterator<Datum[]> inStream, Datum[] from, Datum[] to)
  {
    Datum[] expected = null, found;
    int i = 0;
    if(from != null){
      while(hasNext()){
        i++;
        expected = next();
        if(Datum.compareRows(from, expected) <= 0){ break; }
      }
    } else {
      if(hasNext()){ expected = next(); }
    }
    if(expected == null){ return true; }
    
    while(hasNext() && ((to == null)||(Datum.compareRows(expected, to) <= 0))){
      if(!inStream.hasNext()) { 
        System.err.println("Provided stream ended early");
        return false; 
      }
      i++;
      found = inStream.next();
      if(!Datum.rowEquals(expected, found)){ 
        System.err.println("At Row: " + i);
        System.err.println("Expected: "+Datum.stringOfRow(expected));
        System.err.println("Found: "+Datum.stringOfRow(found));
        return false; 
      }
      expected = next();
    }
    if(to == null){
      if(!inStream.hasNext()) { 
        System.err.println("Provided stream ended early");
        return false; 
      }
      i++;
      found = inStream.next();
      if(!Datum.rowEquals(expected, found)){ 
        System.err.println("At Row: " + i);
        System.err.println("Expected: "+Datum.stringOfRow(expected));
        System.err.println("Found: "+Datum.stringOfRow(found));
        return false; 
      }
    }
    if(inStream.hasNext()){
      System.err.println("Provided stream is too long");
      return false;
    }
    return true;
  }
  
  protected void stepOneKey(boolean reset){
    int tgt = rand.nextInt(curr.length);
    curr[tgt] ++;
    if(reset){
      for(int i = tgt+1; i < curr.length; i++){
        curr[i] = 0;
      }
    }
  }
  protected void stepAllKeys(){
    boolean reset = rand.nextInt(100) >= 70;
    int steps = rand.nextInt(chaos+1);
    for(int i = guaranteeKeyStep ? 0 : 1; i <= steps; i++){
      stepOneKey(reset);
      reset = false;
    }
  }
  
  public boolean hasNext()
  {
    return rows > 0;
  }
  
  public Datum[] next()
  {
    Datum[] row = new Datum[values + curr.length];
    stepAllKeys();
    for(int i = 0; i < row.length; i++){
      if(i < curr.length){
        row[i] = new Datum.Int(curr[i]);
      } else {
        row[i] = new Datum.Int(rand.nextInt(1000));
      }
    }
    rows -= 1;
    return row;
  }
  
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}