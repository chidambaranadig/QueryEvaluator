package edu.buffalo.cse.sql;

import java.util.ArrayList;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.ExprTree.ConstLeaf;

public class ConstEval {

	public static Datum[] consteval(ExprTree expr,ArrayList<Datum[]> table)
	{
		ArrayList<Datum[]> constAns=new ArrayList<Datum[]>();
		Datum[] constData=null;
		switch (expr.op)
		{
		case ADD:
			Datum[] lhsadd=consteval(expr.get(0), table);
			Datum[] rhsadd=consteval(expr.get(1), table);
			switch(lhsadd[0].getType())
			{
			case BOOL: 


			case FLOAT:
				try {
					Datum[]sumfloat=new Datum[]{new Datum.Flt(lhsadd[0].toFloat()+rhsadd[0].toFloat())};
					constData=sumfloat;
				} catch (CastError e1) {

					e1.printStackTrace();
				}
				break;  	
			case INT:

				try {
					Datum[]sumInt=new Datum[]{new Datum.Int(lhsadd[0].toInt()+rhsadd[0].toInt())};
					constData=sumInt;
				} catch (CastError e) {

					e.printStackTrace();
				}
				break;
			default: {}
			}
			int sum=Integer.parseInt(expr.get(0).toString())+Integer.parseInt(expr.get(1).toString());
			constAns.add(new Datum[] {new Datum.Int(sum)}); 
			break;
		case MULT: 
			Datum[] lhsmul=consteval(expr.get(0),table);
			Datum[] rhsmul=consteval(expr.get(1),table);

			switch(lhsmul[0].getType())
			{
			case FLOAT:
				try {
					Datum[]mulfloat=new Datum[]{new Datum.Flt(lhsmul[0].toFloat()*rhsmul[0].toFloat())};
					constData=mulfloat;
				} catch (CastError e1) {

					e1.printStackTrace();
				}
				break;  	
			case INT:

				try {
					Datum[]sumfloat=new Datum[]{new Datum.Int(lhsmul[0].toInt()*rhsmul[0].toInt())};
					constData=sumfloat;
				} catch (CastError e) {

					e.printStackTrace();
				}
				break;
			}
			break;
		case AND:		
			boolean b3=Boolean.parseBoolean(expr.get(0).toString()) && Boolean.parseBoolean(expr.get(1).toString());
			constData=new Datum[] {new Datum.Bool(b3)};
			break;
		case OR:
			Boolean OR=Boolean.parseBoolean(expr.get(0).toString())||Boolean.parseBoolean(expr.get(1).toString());
			constData=new Datum[]{new Datum.Bool(OR)};
			break;
		case NOT:
			constData=new Datum[] {new Datum.Bool(!Boolean.parseBoolean(expr.get(0).toString()))};
			break;
		case CONST:
			ConstLeaf cns=(ConstLeaf)expr;
			Datum[] ans=new Datum[]{cns.v};
			constData=ans;
			constAns.add(new Datum[]{cns.v});
			break;
		}
		return constData;

	}

}
