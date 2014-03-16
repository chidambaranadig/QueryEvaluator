/**
 * A concrete implementation of a relational operator corresponding to a virtual
 * 'null' relation.
 * 
 * You may find this operator useful in implementing SELECT statements with
 * no FROM clause (which produce one tuple by default), or during the later
 * optimization assignments, where it may be useful to have a way to represent a
 * relational operator that produces no data (e.g., SELECT ... WHERE false;)
 *
 **********************************************************
 * You should not need to modify this file for Project 1. *
 **********************************************************
 */

package edu.buffalo.cse.sql.plan;

import java.util.List;
import java.util.ArrayList;

import edu.buffalo.cse.sql.Schema;

public class NullSourceNode extends PlanNode.Leaf {
  public final int rows;
  public NullSourceNode(int rows) 
    { super(PlanNode.Type.NULLSOURCE); this.rows = rows; }
  
  public String detailString()
    { return super.detailString() + "(" + rows + " rows)"; }
  
  public List<Schema.Var> getSchemaVars()
  {
    return new ArrayList<Schema.Var>();
  }
}
