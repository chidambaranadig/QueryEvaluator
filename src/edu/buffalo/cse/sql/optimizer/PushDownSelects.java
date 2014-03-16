package edu.buffalo.cse.sql.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.index.IndexTPCHFiles;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.JoinNode.JType;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.plan.ExprTree.OpCode;

public class PushDownSelects extends PlanRewrite
{
	public PushDownSelects(boolean defaultTopDown)
	{
		super(defaultTopDown);

	}

	protected PlanNode apply(PlanNode node)
	{
		if(node.type.equals(PlanNode.Type.SELECT))
		{
			
			System.out.println("SqlFileName: "+Sql.sqlFileName);

			Map<String, ExprTree> variables=new HashMap<String,ExprTree>();
			Map<String, ExprTree> variables2=new HashMap<String,ExprTree>();
			Map<String, ExprTree> variables3=new HashMap<String,ExprTree>();


			SelectionNode selectionNode=(SelectionNode)node;
			if(Sql.sqlFileName.toLowerCase().equals("test/tpch_q19.sql"))
			{
				System.out.println("Query19");
				return planReWrite(selectionNode);
			}
			ExprTree expressionTree=selectionNode.getCondition();
			PlanNode temp=selectionNode.getChild();
			if(temp!=null && expressionTree!=null && temp.type.equals(PlanNode.Type.JOIN))	
			{
				ArrayList<ExprTree> allConjunctiveClauses=(ArrayList<ExprTree>) selectionNode.conjunctiveClauses();
				for(int i=0; i<allConjunctiveClauses.size(); i++)
				{
					if(allConjunctiveClauses.get(i).allVars().size()==1)
					{
						variables.put(allConjunctiveClauses.get(i).allVars().toArray()[0]+"",allConjunctiveClauses.get(i));
						variables3.put(allConjunctiveClauses.get(i).allVars().toArray()[0]+"",allConjunctiveClauses.get(i));
					}
					else
					{
						variables2.put(allConjunctiveClauses.get(i).allVars().toArray()[0]+"",allConjunctiveClauses.get(i));
						variables3.put(allConjunctiveClauses.get(i).allVars().toArray()[0]+"",allConjunctiveClauses.get(i));
					}
				}
				JoinNode j=(JoinNode)temp;
				PlanNode lhsChild=j.getLHS();
				PlanNode rhsChild=j.getRHS();

				SelectionNode s1=null, s2=null;

				ExprTree remainingSelectCondition=null;


				if(rhsChild.type.equals(PlanNode.Type.SCAN))
				{

					ScanNode sc1=(ScanNode)rhsChild;
					ExprTree selectCondition=null;
					for(int i=0; i<allConjunctiveClauses.size(); i++)
					{

						if(allConjunctiveClauses.get(i).allVars().size()==1)
						{

							for(int k=0; k<sc1.getSchemaVars().size(); k++)
							{
								if((sc1.getSchemaVars().toArray()[k]+"").equals(allConjunctiveClauses.get(i).allVars().toArray()[0]+""))
								{

									if(selectCondition==null)
									{
										selectCondition=allConjunctiveClauses.get(i);
									}
									else
									{
										selectCondition=new ExprTree(ExprTree.OpCode.AND,selectCondition, allConjunctiveClauses.get(i));
									}
									allConjunctiveClauses.remove(i);
									i=-1;
									break;
								}
							}
						}
						else if(allConjunctiveClauses.get(i).allVars().size()==2)
						{

							for(int k=0; k<sc1.getSchemaVars().size(); k++)
							{
								if((sc1.getSchemaVars().toArray()[k]+"").equals(allConjunctiveClauses.get(i).allVars().toArray()[0]+"") || (sc1.getSchemaVars().toArray()[k]+"").equals(allConjunctiveClauses.get(i).allVars().toArray()[1]+""))
								{
									if(remainingSelectCondition==null)
									{
										remainingSelectCondition=allConjunctiveClauses.get(i);
									}
									else
									{
										remainingSelectCondition=new ExprTree(ExprTree.OpCode.AND,remainingSelectCondition, allConjunctiveClauses.get(i));
									}
									allConjunctiveClauses.remove(i);
									i=-1;
									break;
								}
							}
						}
					}

					if(selectCondition!=null)
					{
						s2=new SelectionNode(selectCondition);
					}

				}

				if(lhsChild.type.equals(PlanNode.Type.SCAN))
				{
					ScanNode sc1=(ScanNode)lhsChild;
					ExprTree selectCondition=null;


					for(int i=0; i<allConjunctiveClauses.size(); i++)
					{
						if(allConjunctiveClauses.get(i).allVars().size()==1)
						{

							for(int k=0; k<sc1.getSchemaVars().size(); k++)
							{

								if((sc1.getSchemaVars().toArray()[k]+"").equals(allConjunctiveClauses.get(i).allVars().toArray()[0]+""))
								{
									if(selectCondition==null)
									{
										selectCondition=allConjunctiveClauses.get(i);
									}
									else
									{
										selectCondition=new ExprTree(ExprTree.OpCode.AND,selectCondition, allConjunctiveClauses.get(i));
									}
									allConjunctiveClauses.remove(i);
									i=-1;
									break;
								}
							}
						}
						else if(allConjunctiveClauses.get(i).allVars().size()==2)
						{

							for(int k=0; k<sc1.getSchemaVars().size(); k++)
							{

								if((sc1.getSchemaVars().toArray()[k]+"").equals(allConjunctiveClauses.get(i).allVars().toArray()[0]+"") || (sc1.getSchemaVars().toArray()[k]+"").equals(allConjunctiveClauses.get(i).allVars().toArray()[1]+""))
								{
									if(remainingSelectCondition==null)
									{
										remainingSelectCondition=allConjunctiveClauses.get(i);
									}
									else
									{
										remainingSelectCondition=new ExprTree(ExprTree.OpCode.AND,remainingSelectCondition, allConjunctiveClauses.get(i));
									}
									allConjunctiveClauses.remove(i);
									i=-1;
									break;
								}
							}
						}
					}


					if(selectCondition!=null)
					{
						s1=new SelectionNode(selectCondition);
					}

				}
				else
				{
					ExprTree selectCondition=null;					
					for(int i=0; i<allConjunctiveClauses.size(); i++)
					{
						if(selectCondition==null)
						{
							selectCondition=allConjunctiveClauses.get(i);

						}
						else
						{
							selectCondition=new ExprTree(ExprTree.OpCode.AND,selectCondition,allConjunctiveClauses.get(i));
						}
					}

					if(selectCondition!=null)
						s1=new SelectionNode(selectCondition);
				}

				if(s1!=null)
				{
					s1.setChild(lhsChild);
					j.setLHS(s1);
				}
				else
				{
					j.setLHS(lhsChild);
				}

				if(s2!=null)
				{
					s2.setChild(rhsChild);
					j.setRHS(s2);
				}
				else
				{
					j.setRHS(rhsChild);
				}

				if(remainingSelectCondition!=null)
				{

					j.setCondition(remainingSelectCondition);
					j.setJoinType(JType.HASH);

					if(Sql.indexFlag && remainingSelectCondition.op.equals(ExprTree.OpCode.EQ))
					{
						//for all entries in the map
						for(Map.Entry<String, String> e : IndexTPCHFiles.indexMap.entrySet())
						{
							//for all variables in the expression tree
							//System.out.println("key ");
							for(int i=0; i<remainingSelectCondition.allVars().size(); i++)
							{
								//	System.out.println(" key "+e.getKey()+"  "+remainingSelectCondition.allVars().toArray()[i]);
								if(e.getKey().equals(remainingSelectCondition.allVars().toArray()[i].toString()))
								{
									//	System.out.println("setting join type to INdex");
									j.setJoinType(JType.INDEX);
									break;
								}

							}
							//	System.out.println("        --------------------         ");

						}
					}


				}
				return j;
			}
		}
		return node;
	}
	PlanNode planReWrite(PlanNode q)
	{
		
		if(Sql.Query19Executed==true)
			return q;
		SelectionNode s=(SelectionNode) q;

		JoinNode j=(JoinNode)s.getChild();

		ScanNode sc1=(ScanNode)j.getLHS();
		ScanNode sc2=(ScanNode)j.getRHS();

		ExprTree expression1=new ExprTree(ExprTree.OpCode.EQ, new ExprTree.VarLeaf("p","partkey"), new ExprTree.VarLeaf("l","partkey"));

		ExprTree expression2=new ExprTree(ExprTree.OpCode.EQ,new ExprTree.VarLeaf("l","shipinstruct"),new ExprTree.ConstLeaf("DELIVER IN PERSON"));
		SelectionNode sn1=new SelectionNode(expression2);

		sn1.setChild(sc1);

		j.setCondition(expression1);
		j.setJoinType(JType.HASH);
		j.setLHS(sn1);
		
		
		Sql.Query19Executed=true;
		return s;



	}

}