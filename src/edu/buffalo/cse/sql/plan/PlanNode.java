/**
 * An abstract implementation of functionality common to all nodes in a 
 * relational query plan.  
 * 
 * Three more concrete subclasses of PlanNode are defined:
 *  - Leaf nodes correspond to base relations in the database (i.e., file or 
 *    index scan operators.
 *  - Unary nodes represent relational query operators that read from exactly
 *    one source relation (i.e. selection or projection nodes)
 *  - Binary nodes represent relational query operators that read from two
 *    source relations (i.e., union and join)
 * 
 * Classes for specific operators should implement 
 *  - any functionality required to store operator-specific information
 *    (e.g., the predicate of a selection).
 *  - the getSchema() method
 * 
 * Optionally, specific operators can override the default detailString() method
 * to provide an operator annotated with operator-specific information.
 * 
 **********************************************************
 * You should not need to modify this file for Project 1. *
 **********************************************************
 */

package edu.buffalo.cse.sql.plan;

import java.util.List;

import edu.buffalo.cse.sql.Schema;

public abstract class PlanNode {
  
  /** 
   * An enumerator for classifying the structural type of a plan node
   *  - Leaf nodes read from no relations and should be subclasses of Leaf.
   *  - Unary nodes read from one relation and should be subclasses of Unary.
   *  - Binary nodes read from two relations and should be subclasses of Binary.
   */
  public enum Structure { LEAF, UNARY, BINARY };
  
  /**
   * The explicit type of a plan node.  Like Structure, each type corresponds
   * to a specific class in sql.plan.*, and only subclasses of that should
   * instantiate themselves as being of this type.
   */
  public enum Type { PROJECT, SELECT, SCAN, JOIN, NULLSOURCE, UNION, 
                     AGGREGATE , HJOIN,ISCAN};

  /**
   * An abstract implementation of a PlanNode that reads from one input relation
   * 
   * Only operator-specific methods should need to be implemented in subclasses.
   */
  public static abstract class Unary extends PlanNode {
    protected PlanNode child = null;
    
    public Unary(Type t) { super(Structure.UNARY, t); }
    
    public void setChild(PlanNode child) { this.child = child; }
    public PlanNode getChild() { return this.child; }

    public String toString(String indent)
    {
      return indent + detailString() + " {\n" + 
        child.toString(indent+"  ") + "\n" +
      indent + "}";
    }
  }
  
  /**
   * An abstract implementation of a PlanNode that reads from two input 
   * relations
   *
   * Only operator-specific methods should need to be implemented in subclasses.
   */
  public static abstract class Binary extends PlanNode {
    protected PlanNode lhs = null, rhs = null;
    
    public Binary(Type t) { super(Structure.BINARY, t); }
    
    public void setLHS(PlanNode lhs) { this.lhs = lhs; }
    public PlanNode getLHS() { return this.lhs; }
    
    public void setRHS(PlanNode rhs) { this.rhs = rhs; }
    public PlanNode getRHS() { return this.rhs; }

    public String toString(String indent)
    {
      return indent + detailString() + " {\n" + 
        lhs.toString(indent+"  ") + ", \n" +
        rhs.toString(indent+"  ") + "\n" +
      indent + "}";
    }
  }
  
  
  /**
   * An abstract implementation of a PlanNode that reads from no input relations
   *
   * Only operator-specific methods should need to be implemented in subclasses.
   */
  public static abstract class Leaf extends PlanNode {
    public Leaf(Type t) { super(Structure.LEAF, t); }
    public String toString(String indent)
    {
      return indent + detailString();
    }
  }
  
  public final Structure struct;
  public final Type type;
  
  /**
   * Instantiate a plan node of a given structural type and explicit type.
   *
   * YOU SHOULD NEVER NEED TO CALL THIS YOURSELF.  Instantiate one of the 
   * subclasses of PlanNode instead.
   */
  public PlanNode(Structure struct, Type type){
    this.struct = struct;
    this.type = type;
  }
  
  /**
   * Obtain a detailed summary of the operator-specific information associated
   * with this specific node (and not its children)
   *
   * This method should be overridden by subclasses of PlanNode.
   */
  public String detailString() { return type.toString(); }
  
  /**
   * Compute the schema of the relation defined by this relational operator and
   * its children.
   */
  public abstract List<Schema.Var> getSchemaVars();
  
  /**
   * Generate a string representation of this node and all of its children.
   * 
   * This method should NOT be overridden by subclasses of PlanNode.
   *
   * @param Indent A string of characters that will be prefixed to every line
   *               of text in the returned string.
   */
  public abstract String toString(String indent);
  /**
   * Generate a string representation of this node and all of its children.
   * 
   * This method should NOT be overridden by subclasses of PlanNode.
   */
  public String toString(){ return toString(""); }
}