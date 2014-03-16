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
public class TABLE03 extends TestHarness {
  public static void main(String args[]) {
    TestHarness.main(args, new TABLE03());
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
    ProjectionNode query_0 = new ProjectionNode();
    query_0.addColumn(new ProjectionNode.Column("A", new ExprTree.VarLeaf("R", "A")));
    query_0.addColumn(new ProjectionNode.Column("C", new ExprTree.VarLeaf("S", "C")));
    query_0.setChild(child_1);
    TestHarness.testQuery(tables, getResults0(), query_0);
    System.out.println("Passed RA Test TABLE03");
  }
  public void testSQL() {
    List<List<List<Datum[]>>> expected = new ArrayList<List<List<Datum[]>>>();
    expected.add(getResults0());
    TestHarness.testProgram(new File("test/TABLE03.SQL"),
                            expected);
    System.out.println("Passed SQL Test TABLE03");
  }
  List<List<Datum[]>> getResults0() {
    List<List<Datum[]>> ret = new ArrayList<List<Datum[]>>();
    ret.add(getResultsUD0());
    return ret;
  }
  ArrayList<Datum[]> getResultsUD0() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(1), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(4)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(3), new Datum.Int(5)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(5), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(2)}); 
    ret.add(new Datum[] {new Datum.Int(2), new Datum.Int(1)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(3)}); 
    ret.add(new Datum[] {new Datum.Int(4), new Datum.Int(4)}); 
    return ret;
  }
}
