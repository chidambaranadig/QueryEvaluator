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
public class UNION01 extends TestHarness {
  public static void main(String args[]) {
    TestHarness.main(args, new UNION01());
  }
  public void testRA() {
    Map<String, Schema.TableFromFile> tables
      = new HashMap<String, Schema.TableFromFile>();
    NullSourceNode child_3 = new NullSourceNode(1);
    ProjectionNode lhs_1 = new ProjectionNode();
    lhs_1.addColumn(new ProjectionNode.Column("A", new ExprTree.ConstLeaf(1)));
    lhs_1.setChild(child_3);
    NullSourceNode child_4 = new NullSourceNode(1);
    ProjectionNode rhs_2 = new ProjectionNode();
    rhs_2.addColumn(new ProjectionNode.Column("A", new ExprTree.ConstLeaf(2)));
    rhs_2.setChild(child_4);
    UnionNode query_0 = new UnionNode();
    query_0.setLHS(lhs_1);
    query_0.setRHS(rhs_2);
    TestHarness.testQuery(tables, getResults0(), query_0);
    System.out.println("Passed RA Test UNION01");
  }
  public void testSQL() {
    List<List<List<Datum[]>>> expected = new ArrayList<List<List<Datum[]>>>();
    expected.add(getResults0());
    TestHarness.testProgram(new File("test/UNION01.SQL"),
                            expected);
    System.out.println("Passed SQL Test UNION01");
  }
  List<List<Datum[]>> getResults0() {
    List<List<Datum[]>> ret = new ArrayList<List<Datum[]>>();
    ret.add(getResultsUD0());
    ret.add(getResultsUA0());
    return ret;
  }
  ArrayList<Datum[]> getResultsUD0() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsUA0() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2)}); 
    return ret;
  }
}
