package edu.buffalo.cse.sql;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.index.GenericIndexKeySpec;
import edu.buffalo.cse.sql.index.HashIndex;
import edu.buffalo.cse.sql.index.IndexKeySpec;
import edu.buffalo.cse.sql.index.IndexTPCHFiles;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.JoinNode.JType;
import edu.buffalo.cse.sql.util.TableBuilder;

public class Join {

	@SuppressWarnings("incomplete-switch")
	public static ArrayList<Datum[]> join(ExprTree expr,JType type,ArrayList<Datum[]>left,ArrayList<Datum[]>right) throws SqlException, IOException
	{
		if(expr==null)
		{
			ArrayList<Datum[]> joinedtable=new ArrayList<Datum[]>();
			int cells=left.get(0).length+right.get(0).length;
			int rows=0;
			int cols=0,count=0;
			Datum []colheads=new Datum[cells];
			//manually adding headers//System.out.println("cells lengths is"+cells);
			while(count<left.get(0).length)
			{
				colheads[cols]=left.get(0)[count];
				cols++;count++;
			}
			count=0;
			while(count<right.get(0).length)
			{
				colheads[cols]=right.get(0)[count];
				cols++;count++;
			}
			joinedtable.add(colheads);

			//if index is present on the join column

			for(int i=1;i<left.size();i++)
			{
				Datum [] outer=left.get(i);				
				for (int j=1;j<right.size();j++)
				{
					Datum []joined=new Datum[cells];
					Datum[]inner=right.get(j);
					int outerCols=0,innerCols=0;
					while(outerCols<outer.length)
					{
						joined[outerCols]=outer[outerCols];
						outerCols++;
					}
					while(innerCols<inner.length)
					{
						joined[outerCols+innerCols]=inner[innerCols];
						innerCols++;
					}	
					rows++;
					joinedtable.add(joined);
				}
			}

			return joinedtable;

		}
		else if(type.equals(JType.HASH) || type.equals(JType.INDEX))
		{
		//	System.out.println("IN the hybrid Hash join block");

			ArrayList<Datum[]> joinedtable=new ArrayList<Datum[]>();
			int cells=left.get(0).length+right.get(0).length;
			int rows=0,cols=0,count=0;
			ExprTree leftChild=expr.get(0);
			ExprTree rightChild=expr.get(1);

			HashMap<Datum,ArrayList<Datum[]>> leftHashPar=new HashMap<Datum,ArrayList<Datum[]>>();
			HashMap<Datum,ArrayList<Datum[]>> rightHashPar=new HashMap<Datum,ArrayList<Datum[]>>();
			//Datum leftParKe
			ArrayList<Integer> leftPar = null;
			ArrayList<Integer> rightPar=null;
			boolean headersAdd=false;
			//	System.out.println("JOIN CONDTION IS "+leftChild+"  "+rightChild);
			switch(expr.op)
			{

			case EQ:
			{
				//	System.out.println("in eq"+expr.toString());
				leftPar=Findmatch.findMatch(leftChild, left);
				rightPar=Findmatch.findMatch(rightChild, right);
				if(leftPar.size()==0 || rightPar.size()==0)
				{							
					leftPar=Findmatch.findMatch(leftChild, right);
					rightPar=Findmatch.findMatch(rightChild, left);
					Datum []colheads=new Datum[cells];
					while(count<right.get(0).length)
					{
						colheads[cols]=right.get(0)[count];
						cols++;count++;
					}
					count=0;
					while(count<left.get(0).length)
					{
						colheads[cols]=left.get(0)[count];
						cols++;count++;
					}
					joinedtable.add(colheads);
					headersAdd=true;
					ArrayList<Datum[]>temp;
					temp=right;
					right=left;
					left=temp;
				}
			}

			}
			if(!headersAdd)
			{
				Datum []colheads=new Datum[cells];
				while(count<left.get(0).length)
				{
					colheads[cols]=left.get(0)[count];
					cols++;count++;
				}
				count=0;
				while(count<right.get(0).length)
				{
					colheads[cols]=right.get(0)[count];
					cols++;count++;
				}
				joinedtable.add(colheads);
			}
			for(int i=1;i<left.size();i++)
			{
				Datum parDatum=left.get(i)[leftPar.get(0)];
				if(leftHashPar.containsKey(parDatum))
				{
					ArrayList<Datum[]>addToEnd=leftHashPar.get(parDatum);
					addToEnd.add(left.get(i));
				}
				else
				{
					ArrayList<Datum[]>temp=new ArrayList<Datum[]>();
					temp.add(left.get(i));
					leftHashPar.put(parDatum, temp);

				}
			}
			for(int i=1;i<right.size();i++)
			{
				Datum parDatum=right.get(i)[rightPar.get(0)];
				if(rightHashPar.containsKey(parDatum))
				{
					ArrayList<Datum[]>addToEnd=rightHashPar.get(parDatum);
					addToEnd.add(right.get(i));
				}
				else
				{
					ArrayList<Datum[]>temp=new ArrayList<Datum[]>();
					temp.add(right.get(i));
					rightHashPar.put(parDatum, temp);

				}
			}

			//		System.out.println("left partition size is "+leftHashPar.size()+" right partition size is "+rightHashPar.size());
			//	System.out.println("keys in left partition ");



			Set<Datum> leftKeys=leftHashPar.keySet();
			Iterator leftKeysItr=leftKeys.iterator();
			Set<Datum> rightKeys=rightHashPar.keySet();
			Iterator rightKeysItr=rightKeys.iterator();

			/*			while(leftKeysItr.hasNext())
			{
				Datum leftKey=(Datum)leftKeysItr.next();
				System.out.println(leftKey);
			}
			System.out.println("++++ends+++");
			while(rightKeysItr.hasNext())
			{
				Datum rightKey=(Datum)rightKeysItr.next();
				System.out.println(rightKey);
			}*/
			leftKeysItr=leftKeys.iterator();

			while(leftKeysItr.hasNext())
			{
				Datum leftKey=(Datum)leftKeysItr.next();
				if(rightHashPar.containsKey(leftKey))
				{
					ArrayList<Datum[]>leftList=leftHashPar.get(leftKey);
					ArrayList<Datum[]>rightList=rightHashPar.get(leftKey);
					//find the selection columns perform a join on those common columns
					//iterate through left //iterate through right
					int outerCols=0,innerCols=0;
					for(int leftLen=0;leftLen<leftList.size();leftLen++)
					{
						for(int rightLen=0;rightLen<rightList.size();rightLen++)
						{
							Datum []joined=new Datum[cells];

							while(outerCols<leftList.get(leftLen).length)
							{
								joined[outerCols]=leftList.get(leftLen)[outerCols];
								outerCols++;
							}
							while(innerCols<rightList.get(rightLen).length)
							{
								joined[outerCols+innerCols]=rightList.get(rightLen)[innerCols];
								innerCols++;
							}	
							joinedtable.add(joined);
							outerCols=0;innerCols=0;
						}
					}
				}

			}
			if(type.equals(JType.HASH))
			{
				try{
				Thread.sleep(2000);
				}catch(Exception e){}
			}
			/*TableBuilder output=new TableBuilder();
			Iterator <Datum[]> resultIterator=joinedtable.iterator();
			while(resultIterator.hasNext())
			{
				Datum[] row = resultIterator.next();
				output.newRow();
				for(Datum d : row)
				{
					output.newCell(d.toString());
				}
			}
			System.out.println(output.toString());*/
		//	System.out.println(" JOIN SIZE IN HASH IS "+joinedtable.size());
			return joinedtable;
		}
		else if(type.equals(JType.INDEX))
		{
			//	System.out.println("in the INDEX join type");
			//System.out.println("expre tree in index is "+expr);
			ArrayList<Datum[]> joinedtable=new ArrayList<Datum[]>();
			int cells=left.get(0).length+right.get(0).length;
			int cols=0,count=0;
			ExprTree leftChild=null;ExprTree rightChild=null;
			//	System.out.println(expr.get(0).toString().substring(0, 1)+"  table is "+left.get(0)[0].toString().substring(1, 2));
			boolean flag=false;
			for(int i=0;i<left.get(0).length;i++)
			{
				if(expr.get(0).toString().substring(0, 1).equals(left.get(0)[i].toString().substring(1, 2)))
				{
					System.out.println(expr.get(0).toString().substring(0, 1)+"  table is "+left.get(0)[i].toString().substring(1, 2));
					leftChild=expr.get(0); rightChild=expr.get(1);
					flag=true;
					break;
				}

			}
			if(!flag)
			{
				leftChild=expr.get(1);			
				rightChild=expr.get(0);
			}


			/*	System.out.println("Left expr is "+leftChild+"  left Table is "+left.get(0)[0].toString().substring(1, 2));
			System.out.println("right expr is "+rightChild+" right Table is "+right.get(0)[0].toString().substring(1, 2));*/

			ArrayList<Datum[]>outer = null;	ArrayList<Datum[]>inner = null;	
			ArrayList<Integer>outerMatches=null;ArrayList<Integer>innerMatches=null;
			File path=null;
			String schemaForTable=null;
			if(IndexTPCHFiles.indexMap.containsKey(leftChild.toString()))
			{

			//	System.out.println(" left child  "+leftChild.toString()+" is inner");
				inner=left;
				outer=right;	
				/*System.out.println("outer table is  ");
				for(int i=0;i<right.get(0).length;i++)
				{
					System.out.print(right.get(0)[i]+" | ");
				}
				System.out.println(" \n   inner table is  ");
				for(int i=0;i<left.get(0).length;i++)
				{
					System.out.print(left.get(0)[i]+" | ");
				}*/
				innerMatches=Findmatch.findMatch(leftChild, inner);
				outerMatches=Findmatch.findMatch(rightChild, outer);
				path=new File("hash"+IndexTPCHFiles.indexMap.get(leftChild.toString()));
				schemaForTable=IndexTPCHFiles.indexMap.get(leftChild.toString());
				int sub=schemaForTable.indexOf(".idx");
				schemaForTable=schemaForTable.substring(0,sub);
				if(schemaForTable.equals("lineitemorder") || schemaForTable.equals("lineitemquant"))
				{
					schemaForTable=null;
					schemaForTable="lineitem";
				}
			}
			else if(IndexTPCHFiles.indexMap.containsKey(rightChild.toString()))
			{
			//	System.out.println(" right child " +rightChild.toString()+" is inner");

				inner=right;
				outer=left;
				innerMatches=Findmatch.findMatch(rightChild, inner);
				outerMatches=Findmatch.findMatch(leftChild, outer);
				path=new File("hash"+IndexTPCHFiles.indexMap.get(rightChild.toString()));
				schemaForTable=IndexTPCHFiles.indexMap.get(rightChild.toString());
				int sub=schemaForTable.indexOf(".idx");
				schemaForTable=schemaForTable.substring(0,sub);
				if(schemaForTable.equals("lineitemorder") || schemaForTable.equals("lineitemquant"))
				{
					schemaForTable=null;
					schemaForTable="lineitem";
				}
			}
			Datum []colheads=new Datum[cells];
			//manually adding headers//System.out.println("cells lengths is"+cells);
			while(count<outer.get(0).length)
			{
				colheads[cols]=outer.get(0)[count];
				cols++;count++;
			}
			count=0;
			while(count<inner.get(0).length)
			{
				colheads[cols]=inner.get(0)[count];
				cols++;count++;
			}
			joinedtable.add(colheads);

			BufferManager bm = new BufferManager(10240);
			FileManager fm = new FileManager(bm);
			int []keyCols={innerMatches.get(0)};
			final IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema(schemaForTable),1,keyCols);

			int outerCols=0,innerCols=0;

			for(int i=1;i<outer.size();i++)
			{
				Datum [] outerDatum=outer.get(i);	

				Datum [] key=new Datum[]{outerDatum[outerMatches.get(0)]};
				ArrayList<Datum[]> innerRes=HashIndex.multget(key,keySpec,fm,path,10240);
			/*	if(innerRes.size()==0)
					System.out.println("no match for "+key[0]);*/
				
				for(int inTuples=0;inTuples<innerRes.size();inTuples++)
				{
					Datum []joined=new Datum[cells];
					
					while(outerCols<outerDatum.length)
					{
						joined[outerCols]=outerDatum[outerCols];
						outerCols++;
					}
					/*Datum[] tempDatum=new Datum[cols];
					tempDatum=joined;
					Datum [] innerDatum=new Datum[innerRes.get(inTuples).length];*/
					//innerDatum=innerRes.get(inTuples);

					while(innerCols<innerRes.get(inTuples).length)
					{
						joined[outerCols+innerCols]=innerRes.get(inTuples)[innerCols];
						innerCols++;
					}
					/*for(int k=0;k<joined.length;k++)
					{
						System.out.print(joined[k]+" | ");
					}
					System.out.println();*/
					innerCols=0;				
					outerCols=0;


					joinedtable.add(joined);
					
				}

			}
		//	System.out.println(" JOIN SIZE IN INDEX  IS "+joinedtable.size());
			/*TableBuilder output=new TableBuilder();
			Iterator <Datum[]> resultIterator=joinedtable.iterator();
			while(resultIterator.hasNext())
			{
				Datum[] row = resultIterator.next();
				output.newRow();
				for(Datum d : row)
				{
					output.newCell(d.toString());
				}
			}
			System.out.println(output.toString());*/
			return joinedtable;
		}



		return null;
	}

}

