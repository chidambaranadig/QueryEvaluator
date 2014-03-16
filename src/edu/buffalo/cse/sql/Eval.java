package edu.buffalo.cse.sql;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.util.TableBuilder;

public class Eval {

	public static ArrayList<Datum[]> eval(ExprTree expr,ArrayList<Datum[]> table) throws CastError, IOException
	{
		if(expr.op.equals(ExprTree.OpCode.CONST))
		{
			return table;
		}
		ArrayList<Integer>matches=Findmatch.findMatch(expr, table);
		ArrayList<Datum[]>result=null;
		switch(expr.op)
		{
		case VAR:
		{
	//		System.out.println(" in var "+expr.toString());
			ArrayList<Datum[]> varTable=new ArrayList<Datum[]>();			
			int mats=0;
			int tempVal=0;
			//System.out.println("input size in var is "+table.size());
			int c=0;
			for(int i=0;i<table.size();i++)
			{
				Datum[]cols=table.get(i);
				Datum[] vars=new Datum[matches.size()];
				vars[0]=table.get(i)[matches.get(mats)];
				
				varTable.add(vars);
			}
			//System.out.println("sum is"+c);
			result=varTable;
			break;
		}
		case CONST:
		{
			ArrayList<Datum[]> countTable=new ArrayList<Datum[]>();			
			int mats=0;
			int tempVal=0;
			for(int i=0;i<table.size();i++)  //not excluding the headers
			{
				Datum[]cols=table.get(i);
				Datum[] vars=new Datum[matches.size()];
				vars[0]=table.get(i)[matches.get(mats)];
				countTable.add(vars);
			}
			result=countTable;
			break;
		}
		case ADD:
		{
		//	System.out.println(" expression in add is "+expr.toString());
			ArrayList<Datum[]> addTable=new ArrayList<Datum[]>();			
			int mats=0;
			
			int headsCount=1;
			boolean floatDatum=false;

			for(int i=0;i<table.size();i++)  //excluding the headers
			{
				int tempVal=0;
				float tempValFlt=0;
				
				Datum[]cols=table.get(i);
				if(headsCount==1)
				{
					//	System.out.println("heads 1"+cols[0]);
					headsCount++;
					Datum colHeader[]=new Datum[]{new Datum.Str(expr.toString())};

					addTable.add(colHeader);
					continue;
				}
				if(matches.size()==1)
				{
					try 
					{
						Datum rt=new Datum.Int(Integer.parseInt(expr.get(0).toString()));
						tempVal+=cols[matches.get(mats)].toInt()+rt.toInt();

					} catch (CastError e)
					{
						Datum rt=new Datum.Flt(Float.parseFloat(expr.get(0).toString()));
						tempValFlt+=cols[matches.get(mats)].toFloat()+rt.toFloat();
						floatDatum=true;
					}
					mats=0;

				}
				else
				{
					try 
					{
						tempVal+=cols[matches.get(mats+1)].toInt()+cols[matches.get(mats)].toInt();

					} catch (CastError e)
					{
						tempValFlt+=cols[matches.get(mats+1)].toFloat()+cols[matches.get(mats)].toFloat();
						floatDatum=true;
						//	e.printStackTrace();
					}
					mats=0;
				}
				if(!floatDatum)
				{
					addTable.add(new Datum[]{new Datum.Int(tempVal)});
				}
				else
				{
					addTable.add(new Datum[]{new Datum.Flt(tempValFlt)});

				}

			}

			result=addTable;			
			break;
		}

		case AND:
		{

			ArrayList<Datum[]>lhsAns=eval(expr.get(0),table);
			result=eval(expr.get(1),lhsAns);
			break;
		}
		case DIV:
		{

		}
		case EQ:
		{
		//	FileWriter fw=new FileWriter("Eqal.txt");
			ArrayList<Datum[]> selectionTable=new ArrayList<Datum[]>();		
			int mats=0;int cells=0;int headsCount=1;
			for(int i=0;i<table.size();i++)
			{
				Datum[]cols=table.get(i);
				if(headsCount==1)
				{
					headsCount++;
					selectionTable.add(cols);
					continue;
				}
				if(matches.size()==2)
				{
					if(cols[matches.get(mats)].equals(cols[matches.get(mats+1)]))
					{
						selectionTable.add(cols);
					}
					//cells=0;mats=0;
				}
				else
				{

					if(cols[matches.get(mats)].toString().equals(expr.get(1).toString()))
					{
						//	System.out.println(cols[matches.get(mats)]+"---"+expr.get(1).toString());
						selectionTable.add(cols);
					/*	for(int k=0;k<cols.length;k++)
						{
							fw.write(cols[k]+" | ");
						}
						fw.write("\n");
*/
					}

				}
				cells=0;mats=0;
			}
			result=selectionTable;
			
			break;
		}
		case GT:
		{
			ArrayList<Datum[]> gtTable=new ArrayList<Datum[]>();		
			int mats=0;int cells=0;int headsCount=1;
			for(int i=0;i<table.size();i++)
			{
				Datum[]cols=table.get(i);
				if(headsCount==1)
				{
					headsCount++;
					gtTable.add(cols);
					continue;
				}
				if(matches.size()==2)
				{
					Double lhs=Double.parseDouble(cols[matches.get(mats)].toString());
					Double rhs=Double.parseDouble(cols[matches.get(mats+1)].toString());
					if(lhs>rhs)
					{
						gtTable.add(cols);
					}
				}
				else
				{
					Double lhs=Double.parseDouble(cols[matches.get(mats)].toString());
					Double rhs=Double.parseDouble(expr.get(1).toString());
					//System.out.println("expr    "+expr.get(1));
					if(lhs>rhs)
					{
						gtTable.add(cols);
					}
				}
				cells=0;mats=0;
			}
			result=gtTable;
			break;
		}
		case GTE:
		{
			ArrayList<Datum[]> gteTable=new ArrayList<Datum[]>();		
			int mats=0;int cells=0;int headsCount=1;
			for(int i=0;i<table.size();i++)
			{
				Datum[]cols=table.get(i);
				if(headsCount==1)
				{
					headsCount++;
					//	System.out.println("LHS="+cols[matches.get(mats)]+"  RHS="+cols[matches.get(mats+1)]);
					gteTable.add(cols);
					continue;
				}
				if(matches.size()==2)
				{
					Double lhs=Double.parseDouble(cols[matches.get(mats)].toString());
					Double rhs=Double.parseDouble(cols[matches.get(mats+1)].toString());
					if(lhs>=rhs)
					{
						gteTable.add(cols);
					}
				}
				else
				{
					Double lhs=Double.parseDouble(cols[matches.get(mats)].toString());
					Double rhs=Double.parseDouble(expr.get(1).toString());
					//System.out.println("expr    "+expr.get(1));
					if(lhs>=rhs)
					{
						gteTable.add(cols);
					}
				}
				cells=0;mats=0;

			}
			result=gteTable;
			break;


		}
		case LT:
		{
		//	FileWriter fw=new FileWriter("less.txt");
			ArrayList<Datum[]> ltTable=new ArrayList<Datum[]>();		
			int mats=0;int cells=0;int headsCount=1;
			for(int i=0;i<table.size();i++)
			{
				Datum[]cols=table.get(i);
				if(headsCount==1)
				{
					headsCount++;
					ltTable.add(cols);
					continue;
				}
				if(matches.size()==2)
				{
					Double lhs=Double.parseDouble(cols[matches.get(mats)].toString());
					Double rhs=Double.parseDouble(cols[matches.get(mats+1)].toString());
					if(lhs<rhs)
					{
						ltTable.add(cols);
					}
				}
				else
				{
					Double lhs=Double.parseDouble(cols[matches.get(mats)].toString());
					Double rhs=Double.parseDouble(expr.get(1).toString());
					//System.out.println("expr    "+expr.get(1));
					if(lhs<rhs)
					{
						ltTable.add(cols);
					/*	for(int k=0;k<cols.length;k++)
						{
							fw.write(cols[k]+" | ");
						}
						fw.write("\n");*/

					}
				}
				cells=0;mats=0;
			}
			result=ltTable;
			
			
			break;
		}
		case LTE:
		{
			ArrayList<Datum[]> lteTable=new ArrayList<Datum[]>();		
			int mats=0;int cells=0;int headsCount=1;
			for(int i=0;i<table.size();i++)
			{
				Datum[]cols=table.get(i);
				if(headsCount==1)
				{
					headsCount++;
					lteTable.add(cols);
					continue;
				}
				if(matches.size()==2)
				{
					Double lhs=Double.parseDouble(cols[matches.get(mats)].toString());
					Double rhs=Double.parseDouble(cols[matches.get(mats+1)].toString());
					if(lhs<=rhs)
					{
						lteTable.add(cols);
					}
				}
				else
				{
					Double lhs=Double.parseDouble(cols[matches.get(mats)].toString());
					Double rhs=0.0;
					if(expr.get(1).toString().contains("+"))
					{
						String temp=expr.get(1).toString().replace("(", "");
						temp=temp.replace(")", "");
						String splits[]=temp.split("\\+");
						rhs=Double.parseDouble(splits[0].trim())+Double.parseDouble(splits[1].trim());
					}
					else
					{
					 rhs=Double.parseDouble(expr.get(1).toString());
					//System.out.println("expr    "+expr.get(1));
					}
					if(lhs<=rhs)
					{
						lteTable.add(cols);
					}
				}

				cells=0;mats=0;
			}
			result=lteTable;
			break;

		}
		case MULT:
		{
			//System.out.println("\nexpr type   "+expr.toString()+" \nleft   "+expr.get(0)+" left op is  "+expr.get(0).op+" \n right "+expr.get(1)+" right op is "+expr.get(1).op);
			ArrayList<Datum[]>lhsAns=eval(expr.get(0),table);
			ArrayList<Datum[]>rhsAns=eval(expr.get(1),table);

        float sum=0;
			ArrayList<Datum[]> multTable=new ArrayList<Datum[]>();			
			int mats=0;
			int tempVal=0;
			int headsCount=1;
			for(int i=0;i<lhsAns.size();i++)
			{
				Datum[]leftCols=lhsAns.get(i);
				Datum[]rightCols=rhsAns.get(i);
				tempVal=0;
				if(headsCount==1)
				{
					headsCount++;
					Datum colHeader[]=new Datum[]{new Datum.Str(expr.toString())};
					multTable.add(colHeader);
					continue;
				}
				try 
				{
					tempVal=leftCols[0].toInt()*rightCols[0].toInt();
					multTable.add(new Datum[]{new Datum.Int(tempVal)});
				} catch (CastError e) 
				{

					float tempValFlt = leftCols[0].toFloat()*rightCols[0].toFloat();
					sum+=tempValFlt;
					multTable.add(new Datum[]{new Datum.Flt(tempValFlt)});
					//e.printStackTrace();
				}
				mats=0;

			}
			//System.out.println("multiplication done  "+sum);
			
			
			result=multTable;
			break;
		}
		case NEQ:
		{

		}
		case NOT:
		{


		}
		case OR:
		{
			ArrayList<Datum[]>lhsAns=eval(expr.get(0),table);
			ArrayList<Datum[]>rhsAns=eval(expr.get(1),table);
			ArrayList<Datum[]>orTable=new ArrayList<Datum[]>();
			int cells=lhsAns.get(0).length;
			int rows=0,cols=0,count=0;
			Datum []colheads=new Datum[cells];
			while(count<lhsAns.get(0).length)
			{
				colheads[cols]=lhsAns.get(0)[count];
				cols++;count++;
			}
			count=0;
			orTable.add(colheads);
			for(int i=1;i<lhsAns.size();i++)
			{
				Datum [] lhsDatum=lhsAns.get(i);
				orTable.add(lhsDatum);				
			}
			int rhs=1;
			while(rhs<rhsAns.size())
			{				
				Datum [] rhsDatum=rhsAns.get(rhs);				
				for (int j=1;j<lhsAns.size();j++)
				{
					//check for matching cols
					Datum[]lhsDatum=lhsAns.get(j);
					int innerCols=0;
					while(innerCols<rhsDatum.length)
					{
						if(!rhsDatum[innerCols].equals(lhsDatum[innerCols]))
						{
							break;	
						}
						else
						{
							innerCols++;
						}
					}
					if(innerCols!=lhsAns.get(j).length)
					{
						orTable.add(rhsDatum);
						break;
					}
				}
				rhs++;
			}

			result=orTable;
			break;
		}
		case SUB:
		{
	//		System.out.println(" expression in sub is "+expr.toString());
			ArrayList<Datum[]> subTable=new ArrayList<Datum[]>();			
			int mats=0;
			int tempVal=0;
			int headsCount=1;
			float tempValFlt=0;
			boolean floatDatum=false;
			for(int i=0;i<table.size();i++)  //excluding the headers
			{
				tempValFlt=0;
				tempVal=0;
				Datum[]cols=table.get(i);
				if(headsCount==1)
				{
					//	System.out.println("heads 1"+cols[0]);
					headsCount++;
					Datum colHeader[]=new Datum[]{new Datum.Str(expr.toString())};

					subTable.add(colHeader);
					continue;
				}
				if(matches.size()==2)
				{
					try 
					{
						tempVal+=cols[matches.get(mats+1)].toInt()-cols[matches.get(mats)].toInt();

					} catch (CastError e)
					{
						tempValFlt+=cols[matches.get(mats+1)].toFloat()-cols[matches.get(mats)].toFloat();
						floatDatum=true;
						//	e.printStackTrace();
					}
				}
				else
				{
					try 
					{
						Datum rt=new Datum.Int(Integer.parseInt(expr.get(0).toString()));
						tempVal+=rt.toInt()-cols[matches.get(mats)].toInt();

					} catch (CastError e)
					{
						Datum rt=new Datum.Flt(Float.parseFloat(expr.get(0).toString()));

						tempValFlt+=rt.toFloat()-cols[matches.get(mats)].toFloat();
						floatDatum=true;
						//	e.printStackTrace();
					}
				}
				mats=0;
				if(!floatDatum)
				{
					subTable.add(new Datum[]{new Datum.Int(tempVal)});
				}
				else
				{
					subTable.add(new Datum[]{new Datum.Flt(tempValFlt)});

				}
			}


			result=subTable;			
			break;
		}
		}
		if(result!=null)
		{
			return result;
		}
		else
		{
			System.err.println("ERROR IN EVAL");
			System.exit(-1);
		}
		return null;
	}



}
