/**
 * A concrete implementation of a parse tree of an arbitrary mathematical 
 * expression (which can include floating point, integer, or boolean values).
 *
 * An ExprTree is a tree, where each node is tagged with an OpCode representing
 * how to compute that node's value.  For example the ADD opcode sums up all of
 * its children, while the CONST and VAR opcodes mean a constant value, or a
 * reference to a variable (column/attribute/field/etc...) respectively, and 
 * should have no children. 
 *
 * Every instance of ExprTree is a node in the tree.
 * 
 * Instances of ExprTree with the CONST and VAR opcodes must always be 
 * instances of the ConstLeaf and VarLeaf subclasses. 
 * 
 **********************************************************
 * You should not need to modify this file for Project 1. *
 **********************************************************
 */

package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.data.Datum;

public class ExprTree extends ArrayList<ExprTree> {

  public enum OpCode {
    ADD("+"), MULT("*"), SUB("-"), DIV("/"),
    
    AND("AND"), OR("OR"), NOT("NOT"),
    
    EQ("="), NEQ("<>"), LT("<"), GT(">"), LTE("<="), GTE(">="),
    
    CONST(), VAR();
    
    public final String sep;
    OpCode(String sep) { this.sep = sep; }
    OpCode() { this("<?>"); }
  }
  
  public final OpCode op;
  
  public ExprTree(OpCode op) { this.op = op; }
  public ExprTree(OpCode op, ExprTree child) { this(op); add(child); }
  public ExprTree(OpCode op, ExprTree lhs, ExprTree rhs) 
                                             { this(op,lhs); add(rhs); }
  
  
  /**
   * Get the string representation of this node and all of its children.
   */
  public String toString() {
    boolean first = true;
    switch(op){
      case NOT:  return "(NOT " + this.get(0).toString() + ")";
    }
    StringBuilder sb = new StringBuilder("(");
    for(ExprTree e : this){
      if(first){ first = false; } else { sb.append(" "+op.sep+" "); }
      sb.append(e.toString());
    }
    sb.append(")");
    return sb.toString();
  }
  
  /**
   * Generate an arbitrary name that can be used to reference this expression.
   * 
   * The main purpose of this function is to assign names to fields in a select
   * statement's target clause that do not have an AS associated with them.
   * 
   * For example when parsing the query SELECT name FROM officers,
   * we would need to assign a name to the field returned by the SELECT 
   * statement.  [ExprTree:'officers.name'].makeName() would return 'name'.
   */
  public String makeName() {
    return "EXPR";
  }
  
  /**
   * Return the set of all variables that appear in this expression
   */
  public Set<Schema.Var> allVars()
  { 
    Set<Schema.Var> ret = new HashSet<Schema.Var>(); 
    allVars(ret);
    return ret; 
  }
  
  /**
   * Stack-free aggregation utility function.  Don't call this method.  Use
   * allVars() instead.
   */
  protected void allVars(Set<Schema.Var> aggSet)
    { for(ExprTree child : this) { child.allVars(aggSet); } }
  
  /**
   * Given a mapping from Variable to ExprTree, replace every occurrance of
   * Variable with ExprTree.  Mostly useful in optimizing projection terms and
   * selection predicates.  
   *
   * IMPORTANT: After calling remap() always use the value it returns.  The 
   * original ExprTree will be destroyed, and from its ashes will arise the new
   * rewritten ExprTree node.
   */
  public ExprTree remap(Map<Schema.Var,ExprTree> rewrites)
  {
    for(int i = 0; i < size(); i++){
      set(i, get(i).remap(rewrites));
    }
    return this;
  }
  
  /**
   * An ExprTree node representing a constant value.  ExprTree nodes with opcode
   * CONST must always be of this subclass.
   */
  public static class ConstLeaf extends ExprTree {
    public Datum v; 
    public ConstLeaf(Datum v) { super(OpCode.CONST); this.v = v; }
    public String toString() { return v.toString(); }
    
    public ConstLeaf(int i){ this(new Datum.Int(i)); }
    public ConstLeaf(float f){ this(new Datum.Flt(f)); }
    public ConstLeaf(double f){ this(new Datum.Flt(f)); }
    public ConstLeaf(String s){ this(new Datum.Str(s)); }
    public ConstLeaf(boolean b){ this(new Datum.Bool(b)); }
  }
  
  /**
   * An ExprTree node representing a variable (field reference).  ExprTree nodes 
   * with opcode VAR must always be of this subclass.
   */
  public static class VarLeaf extends ExprTree {
    public final Schema.Var name;
    public VarLeaf(String source, String name) { 
      super(OpCode.VAR); this.name = new Schema.Var(source, name);
    }
    public VarLeaf(String name) { this(null, name); }
    public String toString() { return name.toString(); }
    public String makeName() { return name.name; }
    
    public void allVars(Set<Schema.Var> aggSet) { aggSet.add(name); }
    
    public ExprTree remap(Map<Schema.Var,ExprTree> rewrites)
    {
      return rewrites.get(name);
    }

  }
  
}