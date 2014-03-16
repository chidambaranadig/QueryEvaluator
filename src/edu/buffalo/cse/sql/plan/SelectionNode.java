/**
 * A concrete implementation of a relational plan operator representing a 
 * selection operator.
 * 
 * A selection node has only a single feature: an ExprTree that evaluates to
 * a boolean value when evaluated (e.g., Ship.Name = 'Enterprise').
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

public class SelectionNode extends PlanNode.Unary {
  protected ExprTree condition;
  
  public SelectionNode(ExprTree condition)
      { super(PlanNode.Type.SELECT); this.condition = condition; } 
  
  public void setCondition(ExprTree condition) { this.condition = condition; }
  public ExprTree getCondition() { return condition; }
  
  public String detailString()
      { return super.detailString() + "[" + condition.toString() + "]"; }
  
  public Collection<ExprTree> conjunctiveClauses() 
  {
    ArrayList<ExprTree> conditions = new ArrayList<ExprTree>();
    ArrayList<ExprTree> queue = new ArrayList<ExprTree>();
    queue.add(condition);
    while(queue.size() > 0){
      ExprTree head = queue.remove(0);
      if(head.op == ExprTree.OpCode.AND){ 
        queue.addAll(head);
      } else {
        conditions.add(head);
      }
    }
    return conditions;
  }

  public Collection<ExprTree> disjunctiveClauses() 
  {
    ArrayList<ExprTree> conditions = new ArrayList<ExprTree>();
    ArrayList<ExprTree> queue = new ArrayList<ExprTree>();
    queue.add(condition);
    while(queue.size() > 0){
      ExprTree head = queue.remove(0);
      if(head.op == ExprTree.OpCode.OR){ 
        queue.addAll(head);
      } else {
        conditions.add(head);
      }
    }
    return conditions;
  }
  
  
  
  public List<Schema.Var> getSchemaVars()
  {
    return getChild().getSchemaVars();
  }

  public static SelectionNode make(PlanNode source, ExprTree condition)
  {
    SelectionNode s = new SelectionNode(condition);
    s.setChild(source);
    return s;
  }
}
