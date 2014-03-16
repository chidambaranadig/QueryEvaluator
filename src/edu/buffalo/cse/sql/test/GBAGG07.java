package edu.buffalo.cse.sql.test;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.NullSourceNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.UnionNode;
import edu.buffalo.cse.sql.plan.AggregateNode;
public class GBAGG07 extends TestHarness {
  public static void main(String args[]) {
    TestHarness.main(args, new GBAGG07());
  }
  public void testRA() {
    Map<String, Schema.TableFromFile> tables
      = new HashMap<String, Schema.TableFromFile>();
    Schema.TableFromFile table_S;
    table_S = new Schema.TableFromFile(new File("test/s.dat"));
    table_S.add(new Schema.Column("S", "B", Schema.Type.INT));
    table_S.add(new Schema.Column("S", "C", Schema.Type.INT));
    tables.put("S", table_S);
    Schema.TableFromFile table_R;
    table_R = new Schema.TableFromFile(new File("test/r.dat"));
    table_R.add(new Schema.Column("R", "A", Schema.Type.INT));
    table_R.add(new Schema.Column("R", "B", Schema.Type.INT));
    tables.put("R", table_R);
    ScanNode lhs_3 = new ScanNode("R", "R", table_R);
    ScanNode rhs_4 = new ScanNode("S", "S", table_S);
    JoinNode child_2 = new JoinNode();
    child_2.setLHS(lhs_3);
    child_2.setRHS(rhs_4);
    SelectionNode child_1 = new SelectionNode(new ExprTree(ExprTree.OpCode.EQ, new ExprTree.VarLeaf("R", "B"), new ExprTree.VarLeaf("S", "B")));
    child_1.setChild(child_2);
    AggregateNode query_0 = new AggregateNode();
    query_0.addGroupByVar(new ProjectionNode.Column("A", new ExprTree.VarLeaf(null, "A")));
    query_0.addAggregate(new AggregateNode.AggColumn("Count", new ExprTree.ConstLeaf(1),AggregateNode.AType.COUNT));
    query_0.setChild(child_1);
    TestHarness.testQuery(tables, getResults0(), query_0);
    System.out.println("Passed RA Test GBAGG07-1");
    ScanNode lhs_8 = new ScanNode("R", "R", table_R);
    ScanNode rhs_9 = new ScanNode("S", "S", table_S);
    JoinNode child_7 = new JoinNode();
    child_7.setLHS(lhs_8);
    child_7.setRHS(rhs_9);
    SelectionNode child_6 = new SelectionNode(new ExprTree(ExprTree.OpCode.EQ, new ExprTree.VarLeaf("R", "B"), new ExprTree.VarLeaf("S", "B")));
    child_6.setChild(child_7);
    AggregateNode query_5 = new AggregateNode();
    query_5.addGroupByVar(new ProjectionNode.Column("A", new ExprTree.VarLeaf(null, "A")));
    query_5.addGroupByVar(new ProjectionNode.Column("B", new ExprTree.VarLeaf("R", "B")));
    query_5.addAggregate(new AggregateNode.AggColumn("Count", new ExprTree.ConstLeaf(1),AggregateNode.AType.COUNT));
    query_5.setChild(child_6);
    TestHarness.testQuery(tables, getResults1(), query_5);
    System.out.println("Passed RA Test GBAGG07-2");
    ScanNode lhs_13 = new ScanNode("R", "R", table_R);
    ScanNode rhs_14 = new ScanNode("S", "S", table_S);
    JoinNode child_12 = new JoinNode();
    child_12.setLHS(lhs_13);
    child_12.setRHS(rhs_14);
    SelectionNode child_11 = new SelectionNode(new ExprTree(ExprTree.OpCode.EQ, new ExprTree.VarLeaf("R", "B"), new ExprTree.VarLeaf("S", "B")));
    child_11.setChild(child_12);
    AggregateNode query_10 = new AggregateNode();
    query_10.addGroupByVar(new ProjectionNode.Column("A", new ExprTree.VarLeaf(null, "A")));
    query_10.addGroupByVar(new ProjectionNode.Column("B", new ExprTree.VarLeaf("R", "B")));
    query_10.addGroupByVar(new ProjectionNode.Column("C", new ExprTree.VarLeaf(null, "C")));
    query_10.addAggregate(new AggregateNode.AggColumn("Count", new ExprTree.ConstLeaf(1),AggregateNode.AType.COUNT));
    query_10.setChild(child_11);
    TestHarness.testQuery(tables, getResults2(), query_10);
    System.out.println("Passed RA Test GBAGG07-3");
  }
  public void testSQL() {
    List<List<List<Datum[]>>> expected = new ArrayList<List<List<Datum[]>>>();
    expected.add(getResults0());
    expected.add(getResults1());
    expected.add(getResults2());
    TestHarness.testProgram(new File("test/GBAGG07.SQL"),
                            expected);
    System.out.println("Passed SQL Test GBAGG07");
  }
  List<List<Datum[]>> getResults0() {
    List<List<Datum[]>> ret = new ArrayList<List<Datum[]>>();
    ret.add(getResultsUD0());
    ret.add(getResultsFlipGB0());
    return ret;
  }
  List<List<Datum[]>> getResults1() {
    List<List<Datum[]>> ret = new ArrayList<List<Datum[]>>();
    ret.add(getResultsUD1());
    ret.add(getResultsFlipGB1());
    return ret;
  }
  List<List<Datum[]>> getResults2() {
    List<List<Datum[]>> ret = new ArrayList<List<Datum[]>>();
    ret.add(getResultsUD2());
    ret.add(getResultsFlipGB2());
    return ret;
  }
  ArrayList<Datum[]> getResultsUD0() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(3)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsUD1() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(5), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(5), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(3), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4), new Datum.Int(4)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsUD2() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(2), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(1), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(4), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4), new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(5), new Datum.Int(2), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(5), new Datum.Int(2), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(2), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(3), new Datum.Int(2), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4), new Datum.Int(3), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(3), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1), new Datum.Int(5), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4), new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(3), new Datum.Int(1), new Datum.Int(1)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsFlipGB0() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(5)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsFlipGB1() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(5), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(4), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(2), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(5), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(4), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(3), new Datum.Int(4)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsFlipGB2() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(1), new Datum.Int(3), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(2), new Datum.Int(3), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(4), new Datum.Int(2), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(4), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(5), new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(4), new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(1), new Datum.Int(3), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(2), new Datum.Int(3), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(5), new Datum.Int(3), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(4), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(4), new Datum.Int(2), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(2), new Datum.Int(1), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(4), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(5), new Datum.Int(3), new Datum.Int(1)}); 
    return ret;
  }
}
