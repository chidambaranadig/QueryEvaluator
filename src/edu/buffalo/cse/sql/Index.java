
package edu.buffalo.cse.sql;

import java.io.File;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.index.IndexKeySpec;
import edu.buffalo.cse.sql.index.GenericIndexKeySpec;
import edu.buffalo.cse.sql.index.IndexFile;
import edu.buffalo.cse.sql.index.IndexIterator;
import edu.buffalo.cse.sql.index.ISAMIndex;
import edu.buffalo.cse.sql.index.HashIndex;
import edu.buffalo.cse.sql.test.TestDataStream;

public class Index {
  public enum IndexType { HASH, ISAM };
  
  public static Datum[] parseRow(String row){
    String[] fields = row.split("/, */");
    Datum[] ret = new Datum[fields.length];
    for(int i = 0; i < ret.length; i++){
      ret[i] = new Datum.Int(Integer.parseInt(fields[i]));
    }
    return ret;
  }
  
  public static void main(String[] args)
    throws Exception
  {
    IndexType type = IndexType.HASH;
    int keys = 1;
    int values = 4;
    int rows = 100;
    int frames = 1024;
    int keychaos = 2;
    int indexSize = rows/10;
    boolean validate = false;
    Datum[] get = null;
    Datum[] from = null;
    Datum[] to = null;
    File idxFile = new File("index.dat");
    
    for(int i = 0; i < args.length; i++){
      if(args[i].equals("-keys")){
        keys = Integer.parseInt(args[i+1]);
        i++; 
      } else if(args[i].equals("-values")){
        values = Integer.parseInt(args[i+1]);
        i++; 
      } else if(args[i].equals("-rows")){
        rows = Integer.parseInt(args[i+1]);
        i++; 
      } else if(args[i].equals("-frames")){
        frames = Integer.parseInt(args[i+1]);
        i++; 
      } else if(args[i].equals("-keychaos")){
        keychaos = Integer.parseInt(args[i+1]);
        i++; 
      } else if(args[i].equals("-validate")){
        validate = true;
      } else if(args[i].equals("-get")){
        get = parseRow(args[i+1]);
        i++; 
      } else if(args[i].equals("-from")){
        from = parseRow(args[i+1]);
        i++; 
      } else if(args[i].equals("-to")){
        to = parseRow(args[i+1]);
        i++; 
      } else if(args[i].equals("-indexSize")){
        indexSize = Integer.parseInt(args[i+1]);
        i++; 
      } else if(args[i].equals("-hash")){
        type = IndexType.HASH;
      } else if(args[i].equals("-isam")){
        type = IndexType.ISAM;
      } else {
        idxFile = new File(args[i]);
      }
    }
    
    BufferManager bm = new BufferManager(frames);
    FileManager fm = new FileManager(bm);
    
    TestDataStream ds = new TestDataStream(keys, values, rows, keychaos, true);
    IndexKeySpec keySpec = new GenericIndexKeySpec(ds.getSchema(), keys);
    
    if(validate){
      ManagedFile file = fm.open(idxFile);
      IndexFile idx = null;
      switch(type){
        case HASH:
          System.err.println("HASH Index scan validation unsupported");
          System.exit(-1);
          break;
        case ISAM:
          idx = new ISAMIndex(file, keySpec);
          break;
      }
      IndexIterator scan;
      if(from == null){
        if(to == null){ scan = idx.scan(); }
        else { scan = idx.rangeScanTo(to); }
      } else {
        if(to == null){ scan = idx.rangeScanFrom(from); }
        else { scan = idx.rangeScan(from,to); }
      }
      try {
        if(ds.validate(scan, from, to)){
          System.out.println("Test Successful!");
          System.exit(0);
        } else {
          System.out.println("Test Failed!");
          System.exit(-1);
        }
      } finally {
        scan.close();
      }
    } else if(get != null) {
      ManagedFile file = fm.open(idxFile);
      IndexFile idx = null;
      switch(type){
        case HASH:
          idx = new HashIndex(file, keySpec);
          break;
        case ISAM:
          idx = new ISAMIndex(file, keySpec);
          break;
      }
      
      System.out.println("Getting: "+Datum.stringOfRow(get));
      get = idx.get(get);
      System.out.println("Got: "+((get==null)?"Nothing"
                                             :Datum.stringOfRow(get)));
    
    } else {
      switch(type){
        case HASH:
          HashIndex.create(fm, idxFile, ds, keySpec, indexSize);
          break;
        case ISAM:
          ISAMIndex.create(fm, idxFile, ds, keySpec);
          break;
      }
    }
    
    ManagedFile file = fm.open(idxFile);
    
  }
}