/**
 * Schema-related metadata classes
 * 
 * Type - An enum of all possible data types
 * 
 * Var - A variable name, annotated with its corresponding range variable.
 *
 * Column - A relation attribute, consisting of a full name, and a type
 * 
 * Table - Full metadata for a relation, specified as a (ordered) list of cols
 * 
 * TableFromFile - Extended metadata for a relation defined by a file.
 **/
 
package edu.buffalo.cse.sql;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.io.File;

public class Schema {
  public static enum Type {
    BOOL,
    INT,
    FLOAT,
    DATE,
    STRING
  };
  
  public static class Var {
    public final String rangeVariable;
    public final String name;

    public Var(String rangeVariable, String name) 
      { this.name = name; this.rangeVariable = rangeVariable; }
    public Var(String name) 
      { this(null, name); }
    public String toString()
      { return (rangeVariable == null ? "" : rangeVariable+".") + name; }
    
    public boolean equals(Object o)
    {
      try { return equals((Var)o); } 
      catch(ClassCastException e) { return false; }
    }
    
    public boolean equals(Var v)
    {
      return (rangeVariable == null || v.rangeVariable == null || 
              rangeVariable.equals(v.rangeVariable))
          && (name.equals(v.name));
    }
    
    public int hashCode() { return name.hashCode(); }
  }
  
  public static class Column {
    public final Var name;
    public final Type type;
    
    public Column(String rangeVariable, String name, Type type)
      { this.name = new Var(rangeVariable,name); this.type = type; }
    
    public Column changeRangeVariable(String newRangeVariable){
      return new Column(newRangeVariable, name.name, type);
    }
    
    public String getName() { return name.toString(); }
    public String toString() { return getName() + " " + type.toString(); }
  }
  
  public static class Table extends ArrayList<Column> {
    public Table(){ super(); }
    public Table(Table t){ super(t); }
    
    public Collection<Var> names(){
      ArrayList<Var> names = new ArrayList<Var>();
      for(Column c : this){
        names.add(c.name);
      }
      return names;
    }
    
    public Table changeRangeVariable(String rangeVariable){
      Table t = new Table();
      for(Column c : this){
        t.add(c.changeRangeVariable(rangeVariable));
      }
      return t;
    }
    
    public Column lookup(Var v){
      for(Column c : this){
        if(v.equals(c.name)) { return c; }
      }
      return null;
    }
  }
  
  public static class TableFromFile extends Table {
    final File file;
    public TableFromFile(File file){ super(); this.file = file; }
    public TableFromFile(File file, Table t){ super(t); this.file = file; }
    
    public File getFile() { return file; }
  }
}