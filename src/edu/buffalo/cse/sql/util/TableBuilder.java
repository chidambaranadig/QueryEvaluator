/**
 * A utility class for pretty-printing columnar data.  
 *
 * A sample invocation of TableBuilder looks as follows
 * 
 *   TableBuilder output = new TableBuilder();
 *   for(Schema.Column c : querySchema){
 *     output.newCell(c.getName());
 *     cols++;
 *   }
 *   output.addDividerLine();
 *   while(resultIterator.hasNext()){
 *     Datum[] row = resultIterator.next();
 *     output.newRow();
 *     for(Datum d : row){
 *       output.newCell(d.toString());
 *     }
 *   }
 *
 ************************************************************
 * You should not need to modify this file for Project 1-3. *
 ************************************************************
 */

package edu.buffalo.cse.sql.util;

import java.util.ArrayList;

public class TableBuilder {
  protected final ArrayList<ArrayList<String>> rows = 
      new ArrayList<ArrayList<String>>();
  protected final ArrayList<Integer> colWidths = new ArrayList<Integer>();
  
  protected ArrayList<String> currRow = null;
  
  public TableBuilder() {}
  
  public void newRow() { 
    currRow = new ArrayList<String>();
    rows.add(currRow);
  }
  
  public void newCell(String val){
    int newWidth = val.length();
    if(colWidths.size() < currRow.size() + 1){
      colWidths.add(currRow.size(), newWidth);
    } else {
      int oldWidth = colWidths.get(currRow.size());
      if(newWidth > oldWidth) {
        colWidths.set(currRow.size(), newWidth);
      }
    }
    currRow.add(val);
  }
  
  public void addDividerLine() {
    rows.add(new ArrayList<String>());
    currRow = null;
  }
  
  public String toString(){
    StringBuilder sb = new StringBuilder();
    
    for(ArrayList<String> row : rows){
      String sepStr = " | ";
      String padStr = " ";

      if(row.size() == 0){
        sepStr = "-+-";
        padStr = "-";
      }
      
      sb.append(padStr);
      
      for(int i = 0; i < colWidths.size(); i ++){
        if(i != 0){ sb.append(sepStr); }
        int currWidth = colWidths.get(i);
        String currCell = row.size() > i ? row.get(i) : "";
        sb.append(currCell);
        for(int len = currCell.length(); len < currWidth; len++){
          sb.append(padStr);
        }
      }
      sb.append(padStr);
      sb.append("\n");
    }
    return sb.toString();
  }
}