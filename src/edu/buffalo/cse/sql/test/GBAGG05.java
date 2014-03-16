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
public class GBAGG05 extends TestHarness {
  public static void main(String args[]) {
    TestHarness.main(args, new GBAGG05());
  }
  public void testRA() {
    Map<String, Schema.TableFromFile> tables
      = new HashMap<String, Schema.TableFromFile>();
    Schema.TableFromFile table_R;
    table_R = new Schema.TableFromFile(new File("test/r.dat"));
    table_R.add(new Schema.Column("R", "A", Schema.Type.INT));
    table_R.add(new Schema.Column("R", "B", Schema.Type.INT));
    tables.put("R", table_R);
    ScanNode child_1 = new ScanNode("R", "R", table_R);
    AggregateNode query_0 = new AggregateNode();
    query_0.addGroupByVar(new ProjectionNode.Column("A", new ExprTree.VarLeaf(null, "A")));
    query_0.addAggregate(new AggregateNode.AggColumn("Count", new ExprTree.ConstLeaf(1),AggregateNode.AType.COUNT));
    query_0.setChild(child_1);
    TestHarness.testQuery(tables, getResults0(), query_0);
    System.out.println("Passed RA Test GBAGG05-1");
    ScanNode child_3 = new ScanNode("R", "R", table_R);
    AggregateNode query_2 = new AggregateNode();
    query_2.addGroupByVar(new ProjectionNode.Column("A", new ExprTree.VarLeaf(null, "A")));
    query_2.addGroupByVar(new ProjectionNode.Column("B", new ExprTree.VarLeaf(null, "B")));
    query_2.addAggregate(new AggregateNode.AggColumn("Count", new ExprTree.ConstLeaf(1),AggregateNode.AType.COUNT));
    query_2.setChild(child_3);
    TestHarness.testQuery(tables, getResults1(), query_2);
    System.out.println("Passed RA Test GBAGG05-2");
  }
  public void testSQL() {
    List<List<List<Datum[]>>> expected = new ArrayList<List<List<Datum[]>>>();
    expected.add(getResults0());
    expected.add(getResults1());
    TestHarness.testProgram(new File("test/GBAGG05.SQL"), expected);
    System.out.println("Passed SQL Test GBAGG05");
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
  ArrayList<Datum[]> getResultsUD0() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(2)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsUD1() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(5), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(5), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(3), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(3), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4), new Datum.Int(1)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsFlipGB0() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(5)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsFlipGB1() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(5), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(4), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(2), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(1), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(2), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(5), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(4), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(3), new Datum.Int(4)}); 
    return ret;
  }
}
