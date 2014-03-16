
package edu.buffalo.cse.sql.index;

import java.util.Comparator;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Type;

public class GenericIndexKeySpec implements IndexKeySpec {

	Schema.Type[] dataSchema;
	int[] keyCols;

	public GenericIndexKeySpec(Schema.Type[] dataSchema, int[] keyCols)
	{
	//	System.out.println("calling key Cols");
		this.dataSchema = dataSchema;
		this.keyCols = keyCols;
	}
	public GenericIndexKeySpec(Schema.Type[] dataSchema, int leadingKeys)
	{
		this(dataSchema, leadingKeys(leadingKeys));
	}

	public GenericIndexKeySpec(Schema.Type[] dataSchema, int leadingKeys, int []keyColumn) 
	{

		this(dataSchema, leadingKeys(leadingKeys,keyColumn));

	}
	private static int[] leadingKeys(int leading, int[] keyCol) 
	{

	//	System.out.println("calling leading keys");
		int[] keys = new int[leading];
		for(int i=0;i<leading;i++)
		{
			keys[i] = keyCol[i];
		}
		return keys;
	}
	protected static int[] leadingKeys(int leading)
	{
//		System.out.println("calling leading keys");
		int[] keys = new int[leading];
		for(int i = 0; i < leading; i++){
			keys[i] = i;
		}
		return keys;
	}



	public Datum[] createKey(Datum[] row)
	{
		Datum[] key = new Datum[keyCols.length]; 
		for(int i = 0; i < keyCols.length; i++){
			key[i] = row[keyCols[i]];
		}
		return key;
	}
	public int hashKey(Datum[] key)
	{
		return Datum.hashOfRow(key);
	}
	public int hashRow(Datum[] row)
	{
		return Datum.hashOfRow(createKey(row));
	}
	public int compare(Datum[] a, Datum[] b){
		return Datum.compareRows(a, b);
	}
	public Schema.Type[] rowSchema()
	{ return dataSchema; }
	public Schema.Type[] keySchema()
	{
		Schema.Type[] keySchema = new Schema.Type[keyCols.length];
		for(int i = 0; i < keyCols.length; i++){
			keySchema[i] = dataSchema[keyCols[i]];
		} 
		return keySchema;
	}
}