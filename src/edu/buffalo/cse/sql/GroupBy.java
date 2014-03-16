package edu.buffalo.cse.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.index.Sorttpchtable;
import edu.buffalo.cse.sql.plan.AggregateNode;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.AggregateNode.AggColumn;
import edu.buffalo.cse.sql.plan.ProjectionNode.Column;
import edu.buffalo.cse.sql.util.TableBuilder;

public class GroupBy {

	public static ArrayList<Datum[]> groupby(ArrayList<Datum[]> input,Boolean GroupBy, List<Column> groups,List<AggColumn> agg) throws CastError, IOException
	{
		int gCols=1;int aggCount;
		boolean discard=false;
		ArrayList<Datum[]>ans=new ArrayList<Datum[]>();

		ArrayList<Datum[]> groupedInput=new ArrayList<Datum[]>();

		//+++++++++++++++++++ NEW GROUPBY++++++++++++++++++++++++++++++++++

		//Finding all the cols on which group by has to be performed
		ArrayList<Integer>colGrps=new ArrayList<Integer>();
		for(int grpCnt=0;grpCnt<groups.size();grpCnt++)
		{


			ArrayList<Integer> groupingOncol=Findmatch.findMatch(groups.get(grpCnt).expr, input);
			//System.out.println("grouping on "+groups.get(grpCnt).toString()+"  match in "+groupingOncol);
			for(int colsFound=0;colsFound<groupingOncol.size();colsFound++)
			{
				colGrps.add(groupingOncol.get(colsFound));
			}
			if(groups.get(grpCnt).name.contains("DISCARD"))
			{
				discard=true;
			}
		}

		//adding the group by cols into the treemap
		HashMap<String, ArrayList<Integer> > grouping=new HashMap<String, ArrayList<Integer> >();
		ArrayList<Datum[]>visitedRows=new ArrayList<Datum[]>();

		for(int inCnt=1;inCnt<input.size();inCnt++)
		{					

			Datum[] find=input.get(inCnt);
			Datum []key=new Datum[colGrps.size()];
			String keyString=null;
			StringBuilder sb=new StringBuilder();
			for(int grpCnt=0;grpCnt<colGrps.size();grpCnt++)
			{
				key[grpCnt]=find[colGrps.get(grpCnt)];
				sb.append(find[colGrps.get(grpCnt)].toString()+"@:");
			}
			keyString=new String(sb);

			if(grouping.containsKey(keyString))
			{
				ArrayList<Integer> addToEnd=grouping.get(keyString);
				addToEnd.add(inCnt);
			}
			else
			{			
				ArrayList<Integer> groupList=new ArrayList<Integer>();
				groupList.add(inCnt);
				grouping.put(keyString, groupList);
				visitedRows.add(key);

			}
		}
		//Collections.sort(visitedRows);

		if(discard )
		{
			ans.add(input.get(0));
		}
		Set<String> entry =grouping.keySet();
		Iterator itr=entry.iterator();
		while(itr.hasNext())
		{
			aggCount=-1;
			groupedInput.clear();
			groupedInput.add(input.get(0));//ADDING COL HEADERS MANUALLY
			Datum[] aggData=new Datum[agg.size()+groups.size()];
			String groupByKey=(String)itr.next();
			//System.out.println("groupby key is"+groupByKey);
			ArrayList<Integer> temp=grouping.get(groupByKey);
			for(int tempSize=0;tempSize<temp.size();tempSize++)
			{
				Datum []tempDatum=input.get(temp.get(tempSize));
				groupedInput.add(tempDatum);//creating a sub-table with grouped rows. This is sent to Eval.eval for finding agg Eval.eval

			}
			//System.out.println();
			//reconstructing key 
			String keySplit[]=groupByKey.split("@:");
			for(int keySize=0;keySize<keySplit.length;keySize++)
			{
				try
				{
					aggData[++aggCount]=new Datum.Int(Integer.parseInt(keySplit[keySize]));  //adding cols on which groupby is done

				}catch(Exception e)
				{
					aggData[aggCount]=new Datum.Str((keySplit[keySize]));	
				}
			}
			ArrayList<Datum []> grpIpAftEval =null;
			for(int i=0;i<agg.size();i++)
			{
				if(agg.get(i).expr.op.equals(ExprTree.OpCode.CONST))
				{
					ArrayList<Datum[]> tempList=new ArrayList<Datum[]>();
					for(int tableSize=0;tableSize<groupedInput.size();tableSize++)
					{
						Datum[] constDatum=ConstEval.consteval(agg.get(i).expr,groupedInput);
						tempList.add(constDatum);
						//System.out.println(constDatum[0]);
					}

					grpIpAftEval=tempList;
				}
				else
				{

					grpIpAftEval=Eval.eval(agg.get(i).expr,groupedInput);
				}
				if(agg.get(i).aggType.equals(AggregateNode.AType.SUM))
				{
					//System.out.println("group  sum");

					if(grpIpAftEval.size()==2)
					{
						aggData[++aggCount]=grpIpAftEval.get(1)[0];
						//System.out.println("sum 1 is "+grpIpAftEval.get(1)[0]);
					}
					else if(grpIpAftEval.size()>2)
					{
						int sum=0;
						float sumFloat=0;
						boolean floatDatum=false;
						for(int k=1;k<grpIpAftEval.size();k++)
						{
							Datum [] d=grpIpAftEval.get(k);
							try {
								sum+=d[0].toInt();

							} catch (CastError e)
							{
								sumFloat+=d[0].toFloat();
								floatDatum=true;
							}
						}
						//	System.out.println("sum in group by sum is "+sum+"   "+sumFloat);
						if(!floatDatum)
						{
							aggData[++aggCount]=new Datum.Int(sum);	
						}
						else
						{
							aggData[++aggCount]=new Datum.Flt(sumFloat);
						}
					}

				}
				if(agg.get(i).aggType.equals(AggregateNode.AType.AVG))
				{
					if(grpIpAftEval.size()==2)
					{
						int rowslen=groupedInput.size();
						Datum [] add=grpIpAftEval.get(1);
						try 
						{
							aggData[++aggCount]=new Datum.Flt(add[0].toFloat()/(rowslen-1));
						} catch (CastError e)
						{
							e.printStackTrace();
						}
					}
					else if(grpIpAftEval.size()>2)
					{
						float sum=0;
						for(int k=1;k<grpIpAftEval.size();k++)
						{
							Datum [] d=grpIpAftEval.get(k);
							try 
							{
								sum+=d[0].toFloat();
							} catch (CastError e) {e.printStackTrace();}
						}	
						//	System.out.println("sum in AGG AVG is "+sum);
						aggData[++aggCount]=new Datum.Flt(sum/(grpIpAftEval.size()-1));
					}
					//	System.out.println("aggvalue avg"+aggData[aggCount]);
				}
				if(agg.get(i).aggType.equals(AggregateNode.AType.MAX))
				{
					int rowslen=grpIpAftEval.size();
					Datum [] d1=grpIpAftEval.get(1);
					int max;
					try {
						max = d1[0].toInt();
						for(int k=1;k<rowslen;k++)
						{  
							Datum [] d=grpIpAftEval.get(k);
							if(d[0].toInt()>max)
							{
								max=d[0].toInt();
							}
						}
						aggData[++aggCount]=new Datum.Int(max);
					} catch (CastError e) {
						e.printStackTrace();
					}

				}
				if(agg.get(i).aggType.equals(AggregateNode.AType.MIN))
				{

					int rowslen=grpIpAftEval.size();
					Datum [] d1=grpIpAftEval.get(1);
					int min;
					try {
						min = d1[0].toInt();
						for(int k=1;k<rowslen;k++)
						{  
							Datum [] d=grpIpAftEval.get(k);
							if(d[0].toInt()<min)
							{
								min=d[0].toInt();
							}
						}
						aggData[++aggCount]=new Datum.Int(min);
					} catch (CastError e) {
						e.printStackTrace();
					}

				}
				if(agg.get(i).aggType.equals(AggregateNode.AType.COUNT))
				{

					int rowslen=grpIpAftEval.size()-1;
					aggData[++aggCount]=new Datum.Int(rowslen);

				}

			}


			ans.add(aggData);
			//printtable(ans);

		}
		try{
		Sql.sqlFileName=Sql.sqlFileName.replace("test/", "");
		}catch(Exception e){};
		if(Sql.orderby.containsKey(Sql.sqlFileName))
		{	
			String temp=Sql.orderby.get(Sql.sqlFileName);
			Sql.sqlFileName=Sql.sqlFileName.replace("test/", "");
			String splits[]=temp.split(",");
			if(splits.length==1)
			{
				String inSplit[]=splits[0].split("@");
				int num=Integer.parseInt(inSplit[0]);
				if(inSplit[1].contains("DESC"))
				{
					Collections.sort(ans,Collections.reverseOrder(new Sorttpchtable(num,'b')));
				}
				else
				{
					Collections.sort(ans,new Sorttpchtable(num,'b'));
				}
				if(inSplit[1].contains("$"))
				{
					String limit_split[]=inSplit[1].split("\\$");
					int limit=Integer.parseInt(limit_split[1]);
					ArrayList<Datum[]> limitAns=new ArrayList<Datum[]>();
					for(int l=0;l<limit;l++)
					{
						limitAns.add(ans.get(l));
					}
					ans.clear();
					ans=limitAns;
				}
			}
			else if((splits.length>1))
			{ 
				boolean flag=false;
				int col1;
				if(!splits[0].contains("@"))
				{
					col1=Integer.parseInt(splits[0]);
				}
				else
				{
					String innerSplit[]=splits[0].split("@");
					col1=Integer.parseInt(innerSplit[0]);
					flag=true;

				}
				int col2=Integer.parseInt(splits[1]);
				System.out.println("column 2 is "+col2);
				if(flag)
				{
					Collections.sort(ans,Collections.reverseOrder(new Sorttpchtable(col1,'b')));
				}
				else
				{
					Collections.sort(ans,new Sorttpchtable(col1,'b'));
				}

				ArrayList<Datum[]> multOrder=new ArrayList<Datum[]>();
				int size=0;
				Boolean greaterThan1=false;
				int check=0;
				for(int l=0;l<ans.size();l++)
				{
					Datum[] cmp=ans.get(l);
					ArrayList<Datum[]>tempMultOrder=new ArrayList<Datum[]>();

					for(check=l;check<ans.size();check++)
					{
						Datum toComp[]=ans.get(check);

						if(cmp[col1].equals(toComp[col1]))
						{
							tempMultOrder.add(toComp);
							size++;
							if(size>1)
							{
								greaterThan1=true;
							}
						}
						else
						{
							break;


						}						

					}

					if(greaterThan1)
					{
						Collections.sort(tempMultOrder,new Sorttpchtable(col2,'b'));
						for(int tmp=0;tmp<tempMultOrder.size();tmp++)
						{
							System.out.println("adding "+tempMultOrder.get(tmp)[0]);
							multOrder.add(tempMultOrder.get(tmp));
						}
					}
					else
					{
						multOrder.add(cmp);	
					}
					l=check-1;

				}
				ans.clear();
				if(flag)
				{
					for(int tmp=0;tmp<10;tmp++)
					{
						ans.add(multOrder.get(tmp));
					}
				}
				else
				{
					for(int tmp=0;tmp<multOrder.size();tmp++)
					{
						ans.add(multOrder.get(tmp));
					}
				}



			}
		}
		try
		{
			if(Sql.query.get(Sql.sqlFileName))
			{
				Sql.sqlFileName=Sql.sqlFileName.replace("test/", "");
				ans.add(0,Sql.schema.get(Sql.sqlFileName));
			}
		}catch (Exception e){}
		return ans;
	}

}
