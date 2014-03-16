/**
 * Infrastructure supporting testing functionality.
 *
 **********************************************************
 * You should not need to modify this file for Project 1. *
 **********************************************************
 */

package edu.buffalo.cse.sql.test;

import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import java.io.File;

import edu.buffalo.cse.sql.util.TableBuilder;
import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.Sql;


public abstract class TestHarness {
  
  public abstract void testRA();
  public abstract void testSQL();
  
  public static class IncorrectAnswer extends Exception {
    public IncorrectAnswer(String why){ super(why); }
    
    public String getDetails(){ return getMessage(); }
  }
  
  public static class InvalidResults extends IncorrectAnswer {
    Collection<Datum[]> missingResults;
    Collection<Datum[]> unexpectedResults;
    List<String> colNames;
    public InvalidResults(Collection<Datum[]> missingResults, 
                          Collection<Datum[]> unexpectedResults,
                          List<String> colNames)
    {
      super("Incorrect Results");
      this.missingResults = missingResults;
      this.unexpectedResults = unexpectedResults;
      this.colNames = colNames;
    }
    
    public String getDetails()
    {
      StringBuilder err = new StringBuilder();
      if(missingResults.size() > 0){
        err.append("===== Missing Tuples ======\n");
        err.append(TestHarness.stringOfOutput(missingResults, colNames));
        err.append("\n");
      }
      if(unexpectedResults.size() > 0){
        err.append("===== Incorrect Tuples ======");
        err.append(TestHarness.stringOfOutput(unexpectedResults, colNames));
        err.append("\n");
      }
      return err.toString();
    }
  }
  
  public static String stringOfOutput(Collection<Datum[]> results, 
                                      List<String> colNames)
  {
    TableBuilder output = new TableBuilder();
    output.addDividerLine();
    if(colNames != null){
      output.newRow();
      for(String c : colNames) { output.newCell(c); }
      output.addDividerLine();
    }
    for(Datum[] row : results){
      output.newRow();
      for(Datum cell : row){
        output.newCell(cell.toString());
      }
    }
    return output.toString(); 
  }
  
  public static void validateResult(Collection<Datum[]> results,
                                    List<Datum[]> expectedResults,
                                    List<String> colNames)
    throws IncorrectAnswer
  {
    List<Datum[]> unexpectedResults = new ArrayList<Datum[]>();
    
    for(Datum[] row : results){ 
      // have to iterate manually -- .equals doesn't work on arrays
      int i;
      for(i = 0; i < expectedResults.size(); i++){
        Datum[] cmpRow = expectedResults.get(i);
        if(row.length != cmpRow.length){
          throw new IncorrectAnswer("Result has an incorrect number of fields");
        }
        int j;
        for(j = 0; j < cmpRow.length; j++){
          if(!row[j].equals(cmpRow[j])){ break; }
        }
        if(j >= cmpRow.length) { break; }
      }
      if(i < expectedResults.size()){
        expectedResults.remove(i);
      } else {
        unexpectedResults.add(row);
      }
    }
    if((expectedResults.size() > 0)||(unexpectedResults.size() > 0)){
      throw new InvalidResults(expectedResults, unexpectedResults, colNames);
    }
  }
  
  public static void validateResults(Collection<Datum[]> results,
                                     List<List<Datum[]>> expectedPotResults,
                                     List<String> colNames)
    throws IncorrectAnswer
  {
    IncorrectAnswer ret = null;
    for(List<Datum[]> potResult : expectedPotResults){
      try {
        validateResult(results, potResult, colNames);
        return;
      } catch(IncorrectAnswer e){ ret = e; }
    }
    if(ret != null){ throw ret; }
  }
  
  public static void testQuery(Map<String, Schema.TableFromFile> tables,
                              List<List<Datum[]>> expectedResults,
                              PlanNode query)
  {
    try {
      List<String> colNames = new ArrayList<String>();
      for(Schema.Var v : query.getSchemaVars()){
        colNames.add(v.name);
      }
      validateResults(
        Sql.execQuery(tables, query), expectedResults, colNames
      );
    } catch(IncorrectAnswer e) {
      System.err.println("Processing Query: \n" + query.toString("  "));
      System.err.println("Incorrect Answer: \n" + e.getMessage());
      System.err.println(e.getDetails());
      System.exit(-1);
    } catch(Exception e){
      System.err.println("Processing Query: \n" + query.toString("  "));
      System.err.println("Exception Caught: " + e.getMessage());
      e.printStackTrace(System.err);
      System.exit(-1);
    }
  }
  
  public static void testProgram(File programFile,
                                 List<List<List<Datum[]>>> expectedResults)
  {
    int queryID = 0;
    try {
      List<List<Datum[]>> computedResults = Sql.execFile(programFile);
      
      if(computedResults.size() != expectedResults.size()){
        throw new IncorrectAnswer("Incorrect number of query results (expected "+expectedResults.size()+", but got "+computedResults.size());
      }
      
      for(; queryID < expectedResults.size(); queryID++){
        validateResults(computedResults.get(queryID),
                        expectedResults.get(queryID),
                        null);
      }
    } catch(IncorrectAnswer e) {
      System.err.println("Processing File: \n" + programFile.toString() + " Query #"+queryID);
      System.err.println("Incorrect Answer: \n" + e.getMessage());
      System.err.println(e.getDetails());
      System.exit(-1);
    } catch(Exception e){
      System.err.println("Processing File: \n" + programFile.toString() + " Query #"+queryID);
      System.err.println("Exception Caught: " + e.getMessage());
      e.printStackTrace(System.err);
      System.exit(-1);
    }
    
  }
  
  public static void main(String args[], TestHarness test){
    if(args.length < 1) { test.testRA(); }
    else {
      for(String arg : args){
        arg = arg.toLowerCase();
        if(arg.equals("ra")){ test.testRA(); }
        else if(arg.equals("sql")){ test.testSQL(); }
        else { System.err.println("Unknown argument "+arg); }
      }
    }
  }

}


