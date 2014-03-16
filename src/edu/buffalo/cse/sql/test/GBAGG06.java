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
public class GBAGG06 extends TestHarness {
  public static void main(String args[]) {
    TestHarness.main(args, new GBAGG06());
  }
  public void testRA() {
    Map<String, Schema.TableFromFile> tables
      = new HashMap<String, Schema.TableFromFile>();
    Schema.TableFromFile table_S;
    table_S = new Schema.TableFromFile(new File("test/r.dat"));
    table_S.add(new Schema.Column("S", "B", Schema.Type.INT));
    table_S.add(new Schema.Column("S", "C", Schema.Type.INT));
    tables.put("S", table_S);
    Schema.TableFromFile table_R;
    table_R = new Schema.TableFromFile(new File("test/r.dat"));
    table_R.add(new Schema.Column("R", "A", Schema.Type.INT));
    table_R.add(new Schema.Column("R", "B", Schema.Type.INT));
    tables.put("R", table_R);
    ScanNode lhs_2 = new ScanNode("R", "R", table_R);
    ScanNode rhs_3 = new ScanNode("S", "S", table_S);
    JoinNode child_1 = new JoinNode();
    child_1.setLHS(lhs_2);
    child_1.setRHS(rhs_3);
    AggregateNode query_0 = new AggregateNode();
    query_0.addGroupByVar(new ProjectionNode.Column("A", new ExprTree.VarLeaf(null, "A")));
    query_0.addAggregate(new AggregateNode.AggColumn("Count", new ExprTree.ConstLeaf(1),AggregateNode.AType.COUNT));
    query_0.setChild(child_1);
    TestHarness.testQuery(tables, getResults0(), query_0);
    System.out.println("Passed RA Test GBAGG06-1");
    ScanNode lhs_6 = new ScanNode("R", "R", table_R);
    ScanNode rhs_7 = new ScanNode("S", "S", table_S);
    JoinNode child_5 = new JoinNode();
    child_5.setLHS(lhs_6);
    child_5.setRHS(rhs_7);
    AggregateNode query_4 = new AggregateNode();
    query_4.addGroupByVar(new ProjectionNode.Column("A", new ExprTree.VarLeaf(null, "A")));
    query_4.addGroupByVar(new ProjectionNode.Column("B", new ExprTree.VarLeaf("R", "B")));
    query_4.addAggregate(new AggregateNode.AggColumn("Count", new ExprTree.ConstLeaf(1),AggregateNode.AType.COUNT));
    query_4.setChild(child_5);
    TestHarness.testQuery(tables, getResults1(), query_4);
    System.out.println("Passed RA Test GBAGG06-2");
    ScanNode lhs_10 = new ScanNode("R", "R", table_R);
    ScanNode rhs_11 = new ScanNode("S", "S", table_S);
    JoinNode child_9 = new JoinNode();
    child_9.setLHS(lhs_10);
    child_9.setRHS(rhs_11);
    AggregateNode query_8 = new AggregateNode();
    query_8.addGroupByVar(new ProjectionNode.Column("A", new ExprTree.VarLeaf(null, "A")));
    query_8.addGroupByVar(new ProjectionNode.Column("B", new ExprTree.VarLeaf("R", "B")));
    query_8.addGroupByVar(new ProjectionNode.Column("C", new ExprTree.VarLeaf(null, "C")));
    query_8.addAggregate(new AggregateNode.AggColumn("Count", new ExprTree.ConstLeaf(1),AggregateNode.AType.COUNT));
    query_8.setChild(child_9);
    TestHarness.testQuery(tables, getResults2(), query_8);
    System.out.println("Passed RA Test GBAGG06-3");
  }
  public void testSQL() {
    List<List<List<Datum[]>>> expected = new ArrayList<List<List<Datum[]>>>();
    expected.add(getResults0());
    expected.add(getResults1());
    expected.add(getResults2());
    TestHarness.testProgram(new File("test/GBAGG06.SQL"),
                            expected);
    System.out.println("Passed SQL Test GBAGG06");
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
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(10)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(30)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(10)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(30)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(20)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsUD1() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(5), new Datum.Int(10)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(5), new Datum.Int(10)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(20)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(10)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1), new Datum.Int(10)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(3), new Datum.Int(10)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(20)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4), new Datum.Int(10)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsUD2() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(5), new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(1), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1), new Datum.Int(3), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(2), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(5), new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1), new Datum.Int(2), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(3), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(5), new Datum.Int(3), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(5), new Datum.Int(2), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(3), new Datum.Int(8)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1), new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(4), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(3), new Datum.Int(4), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4), new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(5), new Datum.Int(2), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(5), new Datum.Int(3), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(2), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(5), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(3), new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4), new Datum.Int(4), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(5), new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(5), new Datum.Int(4), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(5), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(2), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(3), new Datum.Int(2), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4), new Datum.Int(3), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(5), new Datum.Int(4), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(5), new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(4), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(3), new Datum.Int(8)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(3), new Datum.Int(3), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4), new Datum.Int(2), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(4), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1), new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4), new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(1), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1), new Datum.Int(4), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(3), new Datum.Int(1), new Datum.Int(1)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsFlipGB0() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(10), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(30), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(10), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(30), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(20), new Datum.Int(5)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsFlipGB1() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(10), new Datum.Int(5), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(10), new Datum.Int(4), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(20), new Datum.Int(2), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(10), new Datum.Int(1), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(10), new Datum.Int(2), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(10), new Datum.Int(5), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(20), new Datum.Int(4), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(10), new Datum.Int(3), new Datum.Int(4)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsFlipGB2() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(4), new Datum.Int(5), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(2), new Datum.Int(3), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(1), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1), new Datum.Int(3), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(5), new Datum.Int(5), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(2), new Datum.Int(1), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(1), new Datum.Int(3), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(4), new Datum.Int(5), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(5), new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(8), new Datum.Int(2), new Datum.Int(3), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(2), new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(4), new Datum.Int(2), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(5), new Datum.Int(3), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(4), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(4), new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(5), new Datum.Int(5), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(3), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(4), new Datum.Int(2), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(1), new Datum.Int(3), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(5), new Datum.Int(3), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(4), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(4), new Datum.Int(5), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(5), new Datum.Int(5), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(3), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(4), new Datum.Int(2), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(5), new Datum.Int(3), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(3), new Datum.Int(4), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(4), new Datum.Int(5), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(5), new Datum.Int(5), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(2), new Datum.Int(3), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(8), new Datum.Int(4), new Datum.Int(2), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(5), new Datum.Int(3), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(4), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(1), new Datum.Int(3), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(2), new Datum.Int(1), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(4), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(4), new Datum.Int(2), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1), new Datum.Int(3), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(2), new Datum.Int(1), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(5), new Datum.Int(3), new Datum.Int(1)}); 
    return ret;
  }
}
