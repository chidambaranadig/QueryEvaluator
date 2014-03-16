package edu.buffalo.cse.sql;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.ProjectionNode.Column;

public class Project {
	public static ArrayList<Datum[]>project(ProjectionNode project,ArrayList<Datum[]>intAns)
	{

		ArrayList<Datum[]>ans=new ArrayList<Datum[]>();
		if(project.getChild().equals(PlanNode.Type.NULLSOURCE))
		{

		}
		else
		{

			List<Column> pCols=project.getColumns();
			String colheads[]=new String[pCols.size()];
			for(int i=0;i<pCols.size();i++)
			{
				colheads[i]=pCols.get(i).expr.toString();
			}
			Datum[]colsHead=intAns.get(0);
			ArrayList<Integer>matches=new ArrayList<Integer>();//store indexes of matching elements
			int index=0;
			int noOfCols=pCols.size();
			while(index<noOfCols)
			{
				for(int j=0;j<colsHead.length;j++)
				{
					if(colsHead[j].toString().contains((colheads[index])))
					{
						matches.add(j);
						break;
					}
				}
				index++;
			}
			int headings=1;//counter to check heading are processed once and not added to the ans
			for(int rowCounter=0;rowCounter<intAns.size();rowCounter++)				
			{
				Datum row[]= intAns.get(rowCounter);
				Datum[] projcols=new Datum[noOfCols];
				if(headings==1)
				{
					int matchcols=0;
					while(matchcols<matches.size())
					{
						projcols[matchcols]=row[matchcols];
						matchcols++;
					}
					headings++;
					continue;
				}
				int matchcols=0;
				while(matchcols<matches.size())
				{
					projcols[matchcols]=row[matches.get(matchcols)];
					matchcols++;
				}
				ans.add(projcols);
			}
		}
		return ans;

	}


}
