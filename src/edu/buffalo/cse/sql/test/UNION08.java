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
public class UNION08 extends TestHarness {
  public static void main(String args[]) {
    TestHarness.main(args, new UNION08());
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
    ScanNode child_3 = new ScanNode("R", "R", table_R);
    ProjectionNode lhs_1 = new ProjectionNode();
    lhs_1.addColumn(new ProjectionNode.Column("B", new ExprTree.VarLeaf(null, "B")));
    lhs_1.setChild(child_3);
    ScanNode child_4 = new ScanNode("S", "S", table_S);
    ProjectionNode rhs_2 = new ProjectionNode();
    rhs_2.addColumn(new ProjectionNode.Column("B", new ExprTree.VarLeaf(null, "B")));
    rhs_2.setChild(child_4);
    UnionNode query_0 = new UnionNode();
    query_0.setLHS(lhs_1);
    query_0.setRHS(rhs_2);
    TestHarness.testQuery(tables, getResults0(), query_0);
    System.out.println("Passed RA Test UNION08");
  }
  public void testSQL() {
    List<List<List<Datum[]>>> expected = new ArrayList<List<List<Datum[]>>>();
    expected.add(getResults0());
    TestHarness.testProgram(new File("test/UNION08.SQL"),
                            expected);
    System.out.println("Passed SQL Test UNION08");
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
    ret.add(new Datum[] {new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(5)}); 
    return ret;
  }
  ArrayList<Datum[]> getResultsUA0() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4)}); 
    return ret;
  }
}
