/**
 * A concrete implementation of a relational plan operator representing a file
 * scan operator.
 *
 **********************************************************
 * You should not need to modify this file for Project 1. *
 **********************************************************
 */
package edu.buffalo.cse.sql.plan;

import java.util.List;
import java.util.ArrayList;

import edu.buffalo.cse.sql.Schema;

public class ScanNode extends PlanNode.Leaf {
  public final String table;
  public final Schema.Table schema;
  
  public ScanNode(String table, String rangeVariable, Schema.Table schema) 
  { 
    super(PlanNode.Type.SCAN); 
    this.table = table; 
    this.schema = schema.changeRangeVariable(rangeVariable);
  }
  public ScanNode(String table, Schema.Table schema) 
  {
    this(table, table, schema);
  }

  public String detailString(){
    StringBuilder sb = new StringBuilder(super.detailString());
    String sep = "";
    
    sb.append(" [");
    sb.append(table);
    sb.append("(");
    for(Schema.Var v : getSchemaVars()){
      sb.append(sep);
      sb.append(v.name);
      sep = ", ";
    }
    sb.append(")]");
    
    return sb.toString();
  }

  public List<Schema.Var> getSchemaVars()
  {
    List<Schema.Var> vars = new ArrayList<Schema.Var>();
    for(Schema.Column c : schema){
      vars.add(c.name);
    }
    return vars;
  }
}
