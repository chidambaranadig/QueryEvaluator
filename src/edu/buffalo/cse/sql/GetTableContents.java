package edu.buffalo.cse.sql;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import edu.buffalo.cse.sql.Schema.TableFromFile;
import edu.buffalo.cse.sql.Schema.Type;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.ScanNode;

public class GetTableContents {

	public static ArrayList<Datum[]> getTableContents(Map<String, Schema.TableFromFile> tables,ScanNode scannode) 
	{
		ArrayList<Datum[]> table=new ArrayList<Datum[]>();	
		for (Map.Entry<String, TableFromFile> entry : tables.entrySet())
		{
			//System.out.println(entry.getKey()+"  "+scannode.table);
			//System.out.println("Table Name: "+entry.getValue().getFile());
			if(entry.getKey().equalsIgnoreCase(scannode.table))
			{
				if(entry.getValue().file.exists())
				{
					try
					{
						BufferedReader br=new BufferedReader(new FileReader(entry.getValue().getFile()));			
						String line;
						int col=0;int count=1;
						int noOfCol=0,colheads=0;

						while((line=br.readLine()) != null)
						{
							//	String split[]=line.split(",|[|]");
							String split[];
							if(scannode.table.equalsIgnoreCase("R") ||scannode.table.equalsIgnoreCase("S")||scannode.table.equalsIgnoreCase("T"))
							{
								split=line.split("\\,");
								noOfCol=split.length;
							//	System.out.println("spliiting by CSV");
							}
							else
							{
								split=line.split("\\|");
								noOfCol=split.length;
						//		System.out.println("splitting by PIPE");
							}
							if(count==1)
							{
								Datum []d=new Datum[noOfCol];
								while(colheads<noOfCol)
								{
									//System.out.println("** "+ scannode.schema.get(colheads).getName());
									d[colheads]=new Datum.Str( scannode.schema.get(colheads).getName());
									colheads++;
								}
								table.add(d);
								count++;
							}
							Datum []d=new Datum[noOfCol];
							for(col=0;col<noOfCol;col++)
							{				
								d[col]=Builddatum(scannode.schema.get(col).type,split[col]);

							}		
							table.add(d);
							//		System.out.println();

						}
					} catch (FileNotFoundException e) 
					{
						e.printStackTrace();
					} catch (IOException e) 
					{
						e.printStackTrace();
					}
				}
				else
				{
					System.err.println(entry.getValue().file+" does not exist!");
					System.exit(-1);
				}

			}
		}
		if(table!=null)
		{
			//Sql.printtable(table);
			return table;
		}
		else 
		{
			System.err.println("ERROR IN GET TABLE CONTENTS");
			System.exit(-1);
		}
		return null;
	}

	public static  Datum Builddatum(Type type,String value)
	{
		//System.out.println(type);
		if(type.equals(Schema.Type.INT))
		{
			value=value.replaceAll("-","");
			Datum temp=new Datum.Int(Integer.parseInt(value));
			return temp;
		}
		else  if(type.equals(Schema.Type.FLOAT))
		{
			Datum temp=new Datum.Flt(Float.parseFloat(value));
			return temp;
		}
		else  if(type.equals(Schema.Type.STRING))
		{
			Datum temp=new Datum.Str(value.trim());
			//	System.out.println("string "+temp.toString());

			return temp;
		}
		else  if(type.equals(Schema.Type.BOOL))
		{
			Datum temp=new Datum.Bool(Boolean.parseBoolean(value));
			return temp;
		}
		return null;

	}
}
