/**
 * A concrete implementation of a relational plan operator representing a 
 * projection operator (technically projection and renaming operators rolled 
 * into one).
 * 
 * A projection node contains a list of String, Expression pairs, defining the
 * name and value of each column (respectively) produced by the projection
 * operator.
 *
 **********************************************************
 * You should not need to modify this file for Project 1. *
 **********************************************************
 */

package edu.buffalo.cse.sql.plan;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import edu.buffalo.cse.sql.Schema;

public class ProjectionNode extends PlanNode.Unary {
  protected String rangeVariable;
  
  public static class Column { 
    public final String name;
    public final ExprTree expr;
    
    public Column(String name, ExprTree expr)
      { this.name = name; this.expr = expr; }
    
    public String toString(){
      return name + ": " + expr.toString();
    }
  }
  
  protected ArrayList<Column> columns = new ArrayList<Column>();
  
  public ProjectionNode(){ super(PlanNode.Type.PROJECT); } 
  
  public void addColumn(Column c){ columns.add(c); }
  public void addColumn(String name, ExprTree expr)
                                 { addColumn(new Column(name, expr)); }
  public List<Column> getColumns(){ return columns; }

  public String getRangeVariable(){ return rangeVariable; }
  public void setRangeVariable(String rangeVariable)
    { this.rangeVariable = rangeVariable; }
  
  public static ProjectionNode make(String rangeVariable,
                                    PlanNode source, 
                                    Collection<Column> columns)
  {
    ProjectionNode p = new ProjectionNode();
    p.setRangeVariable(rangeVariable);
    p.setChild(source);
    for(Column c : columns){ p.addColumn(c); }
    return p;
  }
  
  public String detailString(){
    StringBuilder sb = new StringBuilder(super.detailString());
    sb.append(" [");
    boolean notFirst = false;
    for(Column c : columns) {
      if(notFirst){ sb.append(", "); } else { notFirst = true; }
      sb.append(c.toString());
    }
    sb.append("]");
    return sb.toString();
  }
  
  public List<Schema.Var> getSchemaVars()
  {
    ArrayList<Schema.Var> vars = new ArrayList<Schema.Var>();
    for(Column c : columns){
      vars.add(new Schema.Var(rangeVariable, c.name));
    }
    return vars;
  }
    
  public Map<Schema.Var,ExprTree> getMapping()
  {
    Map<Schema.Var,ExprTree> mapping = new HashMap<Schema.Var,ExprTree>();
    for(Column c : columns){
      Schema.Var out = new Schema.Var(rangeVariable, c.name);
      if(!mapping.containsKey(out)){ mapping.put(out, c.expr); }
    }
    return mapping;
  }
}
