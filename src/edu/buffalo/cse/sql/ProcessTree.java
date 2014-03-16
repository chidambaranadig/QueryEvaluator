package edu.buffalo.cse.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.optimizer.PushDownSelects;
import edu.buffalo.cse.sql.plan.AggregateNode;
import edu.buffalo.cse.sql.plan.IndexScanNode;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.JoinNode.JType;
import edu.buffalo.cse.sql.plan.NullSourceNode;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.plan.UnionNode;

public class ProcessTree {

	public static ArrayList<Datum[]> processTree(PlanNode q,Map<String, Schema.TableFromFile> tables) throws SqlException, IOException 
	{
		/*
		try
		{
			//	System.out.println("\n\nProcessTree: Before Push Down Select\n"+q);
			PushDownSelects pushDownSelectObject= new PushDownSelects(false);
			q=pushDownSelectObject.rewrite(q);
			//	System.out.println("\n\nProcessTree: After Push Down Select Worked\n"+q);
		}
		catch(Exception e)
		{
			System.out.println("PushDownSelect Did not work: "+e);
			e.printStackTrace();
		}*/


		ArrayList<Datum[]> intAns=null;
		if(q.struct.equals(PlanNode.Structure.LEAF))
		{
			if(q.type.equals(PlanNode.Type.SCAN))
			{            
				ScanNode scannode=(ScanNode)q;
				intAns=GetTableContents.getTableContents(tables,scannode);

			}
			else if(q.type.equals(PlanNode.Type.ISCAN))
			{
				IndexScanNode iscannode=(IndexScanNode)q;
				BufferManager bm = new BufferManager(10240);
				FileManager fm = new FileManager(bm);
			//	intAns=ScanIndexFile.ScanIndex(fm,);
			}
			else if(q.type.equals(PlanNode.Type.NULLSOURCE))
			{
				NullSourceNode ns=(NullSourceNode)q;
				ArrayList<Datum[]> nSource=new ArrayList<Datum[]>();
				nSource.add(new Datum[] {new Datum.Int(ns.rows)});
				intAns=nSource;
			}
		}
		else if(q.struct.equals(PlanNode.Structure.BINARY))
		{

			if(q.type.equals(PlanNode.Type.JOIN))
			{
				JoinNode jChild=(JoinNode)q;
				//System.out.println(jChild.getCondition());
				//	System.out.println("size ius "+jChild.getCondition().size());
				if(jChild.getCondition()!=null)
				{
					switch(jChild.getCondition().op)
					{

					case AND:
						//		System.out.println("  AND CONDTION SATISIFIED  ");
						intAns=Join.join(jChild.getCondition().get(0),jChild.getJoinType(),processTree(jChild.getLHS(),tables),processTree(jChild.getRHS(),tables));	
						intAns=Eval.eval(jChild.getCondition().get(1),intAns);
						break;
					case EQ:
						//	System.out.println("NORMAL HASH JOIN BLOCK ");
						intAns=Join.join(jChild.getCondition(),jChild.getJoinType(),processTree(jChild.getLHS(),tables),processTree(jChild.getRHS(),tables));
						break;

					default:
						intAns=Join.join(null,jChild.getJoinType(),processTree(jChild.getLHS(),tables),processTree(jChild.getRHS(),tables));
						intAns=Eval.eval(jChild.getCondition(),intAns);
						break;

					}
				}
				else
				{
					intAns=Join.join(jChild.getCondition(),jChild.getJoinType(),processTree(jChild.getLHS(),tables),processTree(jChild.getRHS(),tables));

				}
			}
			else if(q.type.equals(PlanNode.Type.UNION))
			{
				UnionNode uChild=(UnionNode)q;
				ArrayList<Datum[]>temp1=processTree(uChild.getLHS(),tables);
				ArrayList<Datum[]>temp2=processTree(uChild.getRHS(),tables);			
				ArrayList<Datum[]>union=new ArrayList<Datum[]>();
				for(int i=0;i<temp1.size();i++)
				{
					union.add(temp1.get(i));	
				}
				for(int i=0;i<temp2.size();i++)
				{
					union.add(temp2.get(i));	
				}
				intAns=union;
			}
		}
		else if(q.struct.equals(PlanNode.Structure.UNARY))
		{
			if(q.type.equals(PlanNode.Type.SELECT))
			{				
				SelectionNode sChild=(SelectionNode)q;

				intAns=Eval.eval(sChild.getCondition(),processTree(sChild.getChild(),tables));
			}
			else if(q.type.equals(PlanNode.Type.PROJECT))
			{
				ProjectionNode pChild=(ProjectionNode)q;
				if(pChild.getChild().type.equals(PlanNode.Type.NULLSOURCE))
				{
					intAns=processConstTree(pChild.getChild(), tables);
					ArrayList<Datum[]> temptable=new ArrayList<Datum[]>();
					Datum[] uDatum=new Datum[pChild.getColumns().size()];
					for(int i=0;i<pChild.getColumns().size();i++)
					{

						Datum[] data=ConstEval.consteval(pChild.getColumns().get(i).expr, intAns);
						temptable.add(data);
					}
					for(int i=0;i<temptable.size();i++)
					{
						uDatum[i]=temptable.get(i)[0];
					}
					ArrayList<Datum[]> temp=new ArrayList<Datum[]>();
					temp.add(uDatum);
					intAns=temp;

				}
				else
				{

					intAns=Project.project(pChild,processTree(pChild.getChild(),tables));	
				}
			}
			else if(q.type.equals(PlanNode.Type.AGGREGATE))
			{
				AggregateNode aggChild=(AggregateNode)q;

				System.out.println("done");
				intAns=Agg.Processagg(aggChild,processTree(aggChild.getChild(),tables));	


			}

		}
		return intAns;
	}
	private static ArrayList<Datum[]> processConstTree(PlanNode q,Map<String, Schema.TableFromFile> tables) 
	{
		ArrayList<Datum[]> intAns=null;
		if(q.type.equals(PlanNode.Type.NULLSOURCE))
		{
			NullSourceNode ns=(NullSourceNode)q;
			ArrayList<Datum[]> nSource=new ArrayList<Datum[]>();
			intAns=nSource;
		}

		return intAns;
	}

}
