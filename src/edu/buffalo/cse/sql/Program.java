
package edu.buffalo.cse.sql;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.PlanNode;

public class Program {

  public final HashMap<String, Schema.TableFromFile> tables = new HashMap<String,Schema.TableFromFile>();
  public final List<PlanNode> queries = new ArrayList<PlanNode>();
  
  
  public Program(){}
  public Program(Map<String, Schema.TableFromFile> tables)
  { this.tables.putAll(tables); }
  
  public void addTable(String name, Schema.TableFromFile table) 
    throws SqlException
  {
    if(tables.containsKey(name.toUpperCase())){
      throw new SqlException("Duplicate Table '"+name.toUpperCase()+"'");
    }
    tables.put(name.toUpperCase(), table);
  }
  
  public Schema.TableFromFile getTable(String name)
    throws SqlException
  {
    if(!tables.containsKey(name.toUpperCase())){
      throw new SqlException("Unknown Table '"+name.toUpperCase()+"'");
    }
    return tables.get(name.toUpperCase());
  }
  
  public void addQuery(PlanNode query){
    queries.add(query);
  }
  
  public String toString(){
    StringBuilder sb = new StringBuilder();
    
    for(Schema.TableFromFile t : tables.values()) {
      sb.append(t.toString() + "\n\n");
    }
    
    for(PlanNode q : queries){
      sb.append(q.toString()+"\n\n");
    }
    
    return sb.toString();
  }
}