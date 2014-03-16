/**
 * A concrete implementation of a relational plan operator representing a 
 * aggregate operator.
 *
 **********************************************************
 * You should not need to modify this file for Project 1. *
 **********************************************************
 */

package edu.buffalo.cse.sql.plan;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;

import edu.buffalo.cse.sql.Schema;

public class AggregateNode extends PlanNode.Unary  {
  
  public enum AType { SUM, COUNT, AVG, MIN, MAX }

  public static class AggColumn {
    public final String name;
    public final ExprTree expr;
    public final AType aggType;
    
    public AggColumn(String name, ExprTree expr, AType aggType)
      { this.name = name; this.expr = expr; this.aggType = aggType; }
    
    public String toString(){
      return name + ": " + aggType.toString() + "(" + expr.toString() + ")";
    }
  }
  
  String rangeVariable;
  List<ProjectionNode.Column> groupByVars;
  List<AggColumn> aggregates;
  
  public AggregateNode()
  { 
    super(PlanNode.Type.AGGREGATE); 
    this.rangeVariable = null;
    this.aggregates = new ArrayList<AggColumn>();
    this.groupByVars = new ArrayList<ProjectionNode.Column>();
  }
  public AggregateNode(Collection<ProjectionNode.Column> groupByVars)
  { 
    super(PlanNode.Type.AGGREGATE); 
    this.rangeVariable = null;
    this.aggregates = new ArrayList<AggColumn>();
    this.groupByVars = new ArrayList<ProjectionNode.Column>(groupByVars);
  }
    
  public void setGroupByVars(Collection<ProjectionNode.Column> groupByVars)
    { this.groupByVars = new ArrayList<ProjectionNode.Column>(groupByVars); }
  public List<ProjectionNode.Column> getGroupByVars()
    { return groupByVars; }
  public void addGroupByVar(ProjectionNode.Column col)
    { groupByVars.add(col); }
  public void addAggregate(AggColumn col)
    { aggregates.add(col); }
  public List<AggColumn> getAggregates()
    { return aggregates; }

  public String getRangeVariable(){ return rangeVariable; }
  public void setRangeVariable(String rangeVariable)
    { this.rangeVariable = rangeVariable; }
  
  public List<Schema.Var> getSchemaVars()
  {
    ArrayList<Schema.Var> ret = new ArrayList<Schema.Var>();
    for(ProjectionNode.Column gbv : groupByVars){
      ret.add(new Schema.Var(rangeVariable, gbv.name));
    }
    for(AggColumn col : aggregates){
      ret.add(new Schema.Var(rangeVariable, col.name));
    }
    return ret;
  }
  
  public String detailString(){
    StringBuilder sb = new StringBuilder(super.detailString());
    sb.append(" [");
    boolean notFirst = false;
    for(ProjectionNode.Column c : groupByVars) {
      if(notFirst){ sb.append(", "); } else { notFirst = true; }
      sb.append(c.toString());
    }
    for(AggColumn agg : aggregates) {
      if(notFirst){ sb.append(", "); } else { notFirst = true; }
      sb.append(agg.toString());
    }
    sb.append("]");
    return sb.toString();
  }
    
  public static AggregateNode make(
         String rangeVariable, 
         PlanNode source,
         Collection<ProjectionNode.Column> groupByVars,
         Collection<AggColumn> aggregates)
  {
    AggregateNode a = new AggregateNode(groupByVars);
    for(AggColumn agg : aggregates){ a.addAggregate(agg); }
    a.setChild(source);
    return a;
  }
}
