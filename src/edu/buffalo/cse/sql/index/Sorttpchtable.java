package edu.buffalo.cse.sql.index;
import java.util.Comparator;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;

public class Sorttpchtable implements Comparator<Datum[]> {

	int comp;
	char table;
	public Sorttpchtable(int comp, char table) 
	{
		this.comp=comp;
		this.table=table;
	}

	@SuppressWarnings("incomplete-switch")
	public int compare(Datum[] lt, Datum[] rt) 
	{

		if(lt[comp].equals(rt[comp])) return 0;
		try 
		{
			switch(lt[comp].getType()){
			case INT: 
				switch(lt[comp].getType()){
				case INT: return lt[comp].toInt() > rt[comp].toInt() ? 1 : -1;
				case FLOAT: return lt[comp].toFloat() > rt[comp].toFloat() ? -1 : 1;
				case STRING: return -1;
				case BOOL: return -1;
				}
			case FLOAT:
				switch(lt[comp].getType()){
				case INT: 
				case FLOAT: return lt[comp].toFloat() > rt[comp].toFloat() ? 1 : -1;
				case STRING: return -1;
				case BOOL: return -1;
				}
			case STRING: 
				switch(lt[comp].getType()){
				case INT: 
				case FLOAT: 
				case BOOL: return 1;
				case STRING: return lt[comp].toString().compareTo(rt[comp].toString());
				}
			case BOOL:
				switch(lt[comp].getType()){
				case INT: 
				case FLOAT: return 1;
				case BOOL: return lt[comp].toBool() ? -1 : 1;
				case STRING: return -1;
				}
			default:
				break;        
			} 
		} catch (CastError e){}
		return 0;
	}

	
}
