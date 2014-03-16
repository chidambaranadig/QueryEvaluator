package edu.buffalo.cse.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.AggregateNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.AggregateNode.AggColumn;

public class Agg {

	public static ArrayList<Datum []>  Processagg(AggregateNode a, ArrayList<Datum[]> intAns) throws CastError, IOException 
	{
		ArrayList<Datum []> ans=new ArrayList<Datum[]>();
		List<AggColumn> agg=a.getAggregates();
		List<ProjectionNode.Column> groups=a.getGroupByVars();
		Datum[] aggData=null;
		int groupvars=0;
		groupvars=groups.size();
		List<Datum []> groupByAns=null;

		if(groupvars>0)
		{
			aggData=new Datum[groupvars+1];
		}
		else 
		{
			aggData=new Datum[agg.size()];
		}
		int aggCount=0;//global counter that operates on agg columns
		boolean groupByExecuted=false;
		for(int i=0;i<agg.size();i++)
		{

			if(agg.get(i).aggType==AggregateNode.AType.SUM)
			{					
				int sum=0;
				ArrayList<Datum []> sumAns;
				//ArrayList<Datum[]>groupAns=new ArrayList<Datum[]>();
				sumAns=Eval.eval(agg.get(i).expr,intAns);
				if(groupvars>0 & groupByExecuted==false)
				{			
					groupByExecuted=true;
					groupByAns=GroupBy.groupby(intAns,true,groups,agg);

				}//group by ends
				else if(groupByExecuted==false)
				{
					if(sumAns.size()==2)
					{
						aggData[aggCount++]=sumAns.get(1)[0];
					}
					else if(sumAns.size()>2)
					{
						sum=0;
						float sumFlt=0;
						boolean floatDatum=false;
						if(sumAns.get(0).length==1);
						for(int k=1;k<sumAns.size();k++)
						{
							Datum [] d=sumAns.get(k);
							try
							{
								sum+=d[0].toInt();
							}catch(CastError e)
							{
								sumFlt+=d[0].toFloat();
								floatDatum=true;
							}

						}
						if(!floatDatum)
						{
							aggData[aggCount++]=new Datum.Int(sum);
						}
						else
						{
							aggData[aggCount++]=new Datum.Flt(sumFlt);
						}
					}
				}
			}
			if(agg.get(i).aggType==AggregateNode.AType.AVG)
			{

				float sum=0;
				ArrayList<Datum []> avgAns;
				avgAns=Eval.eval(agg.get(i).expr,intAns);
				if(groupvars>0 & groupByExecuted==false)
				{    
					groupByExecuted=true;
					groupByAns=GroupBy.groupby(intAns,null,groups,agg);

					//	System.out.println("temp size is"+temp.size());
				}//group by ends
				else if(groupByExecuted==false)
				{
					if(avgAns.size()==2)
					{
						int rowslen=intAns.size();
						Datum [] add=avgAns.get(1);
						aggData[aggCount++]=new Datum.Flt(add[0].toFloat()/(rowslen-1));
					}
					else if(avgAns.size()>2)
					{
						int rowslen=intAns.size();
						sum=0;
						for(int k=1;k<avgAns.size();k++)
						{
							Datum [] d=avgAns.get(k);
							sum+=d[0].toFloat();
						}
						aggData[aggCount++]=new Datum.Flt(sum/(rowslen-1));
					}
				}
			}
			if(agg.get(i).aggType==AggregateNode.AType.MAX)
			{
				//FInd the maximum of the data
				if(groupvars>0 & groupByExecuted==false)
				{
					groupByExecuted=true;
					groupByAns=GroupBy.groupby(intAns,null,groups,agg);
				}//group by ends
				else if(groupByExecuted==false)
				{
					int rowslen=intAns.size();

					Datum [] d1=intAns.get(1);
					int max=d1[0].toInt();
					for(int k=1;k<rowslen;k++)
					{  
						Datum [] d=intAns.get(k);
						if(d[0].toInt()>max)
						{
							max=d[0].toInt();
						}

					}
					aggData[aggCount++]=new Datum.Int(max);


				}
			}
			if(agg.get(i).aggType==AggregateNode.AType.MIN)
			{
				if(groupvars>0 & groupByExecuted==false)
				{
					groupByExecuted=true;
					groupByAns=GroupBy.groupby(intAns,null,groups,agg);

				}//group by ends
				else if(groupByExecuted==false)
				{
					int rowslen=intAns.size();
					ArrayList<Integer>cols=Findmatch.findMatch(agg.get(i).expr, intAns);
					System.out.println(cols);
					Datum [] d1=intAns.get(1);
					int min=d1[0].toInt();
					for(int k=1;k<rowslen;k++)
					{  
						Datum [] d=intAns.get(k);
						if(d[0].toInt()<min)
						{
							min=d[0].toInt();
						}
					}
					aggData[aggCount++]=new Datum.Int(min);


				}
			}
			if(agg.get(i).aggType==AggregateNode.AType.COUNT)
			{
				if(groupvars>0 & groupByExecuted==false)
				{
					groupByExecuted=true;
					groupByAns=GroupBy.groupby(intAns,null,groups,agg);

				}//group by ends
				else if(groupByExecuted==false)
				{
					int rowslen=intAns.size();
					aggData[aggCount++]=new Datum.Int(rowslen-1);
				}
			}
		}
		ans.add(aggData);
		aggCount=0;//resetting the counter
		if(groupvars>0)
		{
			ans=(ArrayList<Datum[]>) groupByAns;
		}
		else
		{

		}
		return ans;

	}

}
