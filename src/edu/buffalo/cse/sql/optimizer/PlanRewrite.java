
package edu.buffalo.cse.sql.optimizer;

import edu.buffalo.cse.sql.SqlException;

import edu.buffalo.cse.sql.plan.PlanNode;

public abstract class PlanRewrite
{

	protected boolean defaultTopDown;

	protected PlanRewrite(boolean defaultTopDown)
	{ 
		this.defaultTopDown = defaultTopDown; 
		}

	protected abstract PlanNode apply(PlanNode node) throws SqlException;

	public PlanNode rewriteTopDown(PlanNode node) throws SqlException
	{
		node = apply(node);
		
		switch(node.struct){
		case LEAF: break;
		case UNARY: {
			PlanNode.Unary unode = (PlanNode.Unary)node;
			unode.setChild(rewriteTopDown(unode.getChild()));
		} 
		break;
		case BINARY: {
			PlanNode.Binary bnode = (PlanNode.Binary)node;
			bnode.setLHS(rewriteTopDown(bnode.getLHS()));
			bnode.setRHS(rewriteTopDown(bnode.getRHS()));
		} 
		break;
		}
		return node;
	}

	public PlanNode rewriteBottomUp(PlanNode node) throws SqlException
	{
		switch(node.struct)
		{
		case LEAF: break;
		case UNARY:
		{
			PlanNode.Unary unode = (PlanNode.Unary)node;
			unode.setChild(rewriteTopDown(unode.getChild()));
		} 
		break;
		case BINARY:
		{
			PlanNode.Binary bnode = (PlanNode.Binary)node;
			bnode.setLHS(rewriteTopDown(bnode.getLHS()));
			bnode.setRHS(rewriteTopDown(bnode.getRHS()));
		} 
		break;
		}
		node = apply(node);
		return node;
	}

	public PlanNode rewrite(PlanNode node) throws SqlException
	{ 
		if(defaultTopDown) { return rewriteTopDown(node); }
		else { return rewriteBottomUp(node); }
	}

}
