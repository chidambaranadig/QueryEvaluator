package edu.buffalo.cse.sql;

import java.util.ArrayList;
import java.util.Set;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.ExprTree;

public  class Findmatch
{

	public static  ArrayList<Integer> findMatch(ExprTree expr,ArrayList<Datum[]> table)
	{
		Set<Schema.Var> set=expr.allVars();
		int varLength=set.size();//no of expressions in the expression tree;
		String colheads[]=new String[varLength];
		int j=0;int size=set.size();//adding in the reverse order
		for (Schema.Var s : set) 
		{
			//System.out.println("colheads are "+s.name+"   "+s.toString());
			colheads[--size]=s.toString();
		}
		j=0;int index=0;
		Datum[]colsHead=table.get(0);
		ArrayList<Integer>matches=new ArrayList<Integer>();//store indexes of matching elements
		while(index<varLength)
		{
			for(j=0;j<colsHead.length;j++)
			{
				if(colsHead[j].toString().contains((colheads[index])))
				{
					matches.add(j);
					break;
				}
			}
			index++;
		}
		if(matches!=null)
		{
			return matches;
		}
		else
		{
			System.err.println("ERROR IN FINDMATCH");
			System.exit(-1);	
		}
		return null;
	}

}
