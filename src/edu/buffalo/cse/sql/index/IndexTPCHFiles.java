package edu.buffalo.cse.sql.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.GetTableContents;
import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;

public class IndexTPCHFiles
{
	public static Schema.Type[] customerSchema;
	public static Schema.Type[] lineItemSchema;
	public static Schema.Type[] partSchema;
	public static Schema.Type[] nationSchema;
	public static Schema.Type[] ordersSchema;
	public static Schema.Type[] regionSchema;
	public static Schema.Type[] supplierSchema;
	public static Schema.Type[] partsSupplierSchema;


	public static ISAMIndex customerIndex;
	public static ISAMIndex lineItemIndex;
	public static ISAMIndex lineItemOrdersIndex;
	public static ISAMIndex lineItemQuantIndex;
	public static ISAMIndex partIndex;
	public static ISAMIndex nationIndex;
	public static ISAMIndex ordersIndex;
	public static ISAMIndex regionIndex;
	public static ISAMIndex supplierIndex;
	public static ISAMIndex partsSupplierIndex;


	public static HashIndex hashcustomerIndex;
	public static HashIndex hashlineItemIndex;
	public static HashIndex hashlineItemOrdersIndex;
	public static HashIndex hashlineItemQuantIndex;

	public static HashIndex hashpartIndex;
	public static HashIndex hashnationIndex;
	public static HashIndex hashordersIndex;
	public static HashIndex hashregionIndex;
	public static HashIndex hashsupplierIndex;
	public static HashIndex hashpartsSupplierIndex;

	public static int fc=0;

	public static HashMap<String ,String> indexMap=new HashMap<String,String>();

	public static Schema.Type[] getSchema(String table)
	{

		if(table.equals("lineitem"))
		{
			Schema.Type[] lineItemSchema;
			lineItemSchema=new Schema.Type[16];
			lineItemSchema[0] = Schema.Type.INT;		//orderkey int
			lineItemSchema[1] = Schema.Type.INT;		//partkey int
			lineItemSchema[2] = Schema.Type.INT;		//suppkey int
			lineItemSchema[3] = Schema.Type.INT;		//linenumber int
			lineItemSchema[4] = Schema.Type.FLOAT; 		// quantity float
			lineItemSchema[5] = Schema.Type.FLOAT;		//extended price float
			lineItemSchema[6] = Schema.Type.FLOAT;		//discount float
			lineItemSchema[7] = Schema.Type.FLOAT;		//tax float
			lineItemSchema[8] = Schema.Type.STRING;		//returnflag string
			lineItemSchema[9] = Schema.Type.STRING;		//linestatus string
			lineItemSchema[10] = Schema.Type.INT;		//shipdate int
			lineItemSchema[11] = Schema.Type.INT;		//commitdate int
			lineItemSchema[12] = Schema.Type.INT;		//receiptdate int
			lineItemSchema[13] = Schema.Type.STRING;	//shipinstruct string
			lineItemSchema[14] = Schema.Type.STRING;	//shipmode string
			lineItemSchema[15] = Schema.Type.STRING;	//comment string

			return lineItemSchema;
		}
		else if(table.equals("part"))
		{
			partSchema=new Schema.Type[9];

			partSchema[0] = Schema.Type.INT; 
			partSchema[1] = Schema.Type.STRING;
			partSchema[2] = Schema.Type.STRING;
			partSchema[3] = Schema.Type.STRING;
			partSchema[4] = Schema.Type.STRING;
			partSchema[5] = Schema.Type.INT;
			partSchema[6] = Schema.Type.STRING;
			partSchema[7] = Schema.Type.FLOAT;
			partSchema[8] = Schema.Type.STRING;

			return partSchema;
		}
		else if(table.equals("customer"))
		{
			customerSchema=new Schema.Type[8];

			customerSchema[0] = Schema.Type.INT; 	//custkey int
			customerSchema[1] = Schema.Type.STRING;	//name string
			customerSchema[2] = Schema.Type.STRING;	//address string
			customerSchema[3] = Schema.Type.INT;	//nationkey int
			customerSchema[4] = Schema.Type.STRING;	//phone string
			customerSchema[5] = Schema.Type.FLOAT;	//accountbalance float
			customerSchema[6] = Schema.Type.STRING;	//mktsegment string
			customerSchema[7] = Schema.Type.STRING;	//comment string

			return customerSchema;
		}
		else if(table.equals("supplier"))
		{
			supplierSchema=new Schema.Type[8];

			supplierSchema[0] = Schema.Type.INT;	//suppkey int
			supplierSchema[1] = Schema.Type.STRING;	//name string
			supplierSchema[2] = Schema.Type.STRING;	//address string
			supplierSchema[3] = Schema.Type.INT;	//nationkey int
			supplierSchema[4] = Schema.Type.STRING;	//phone string
			supplierSchema[5] = Schema.Type.FLOAT; 	//accountbalance int(found int in TPCH-5)
			supplierSchema[6] = Schema.Type.STRING;	//comment string


			return supplierSchema;
		}
		else if(table.equals("partsupp"))
		{
			partsSupplierSchema=new Schema.Type[5];

			partsSupplierSchema[0] = Schema.Type.INT; //
			partsSupplierSchema[1] = Schema.Type.INT;
			partsSupplierSchema[2] = Schema.Type.INT;
			partsSupplierSchema[3] = Schema.Type.FLOAT;
			partsSupplierSchema[4] = Schema.Type.STRING;

			return partsSupplierSchema;
		}
		else if(table.equals("orders"))
		{
			ordersSchema=new Schema.Type[9];

			ordersSchema[0] = Schema.Type.INT; 		//orderkey int
			ordersSchema[1] = Schema.Type.INT;		//custkey int
			ordersSchema[2] = Schema.Type.STRING;	//orderstatus string
			ordersSchema[3] = Schema.Type.FLOAT;	//totalprice float
			ordersSchema[4] = Schema.Type.INT;		//orderdate int
			ordersSchema[5] = Schema.Type.STRING;	//orderpriority string
			ordersSchema[6] = Schema.Type.STRING;	//clerk string
			ordersSchema[7] = Schema.Type.INT;		//shippriority int
			ordersSchema[8] = Schema.Type.STRING;	//comment string

			return ordersSchema;
		}
		else if(table.equals("region"))
		{
			regionSchema=new Schema.Type[3];

			regionSchema[0] = Schema.Type.INT; 		//regionkey int
			regionSchema[1] = Schema.Type.STRING;	//name string
			regionSchema[2] = Schema.Type.STRING;	//comment string
			return regionSchema;
		}
		else if(table.equals("nation"))
		{
			nationSchema=new Schema.Type[4];

			nationSchema[0] = Schema.Type.INT; 		//nationkey int
			nationSchema[1] = Schema.Type.STRING;	//name string
			nationSchema[2] = Schema.Type.INT;		//regionkey int
			nationSchema[3] = Schema.Type.STRING;	//comment string
			return nationSchema;
		}

		return null;

	}
	public static int indexCol=0;
	public static char table;
	public static void indexFiles(String filename)
	{


	 if(filename.contains("TPCH_Q1.SQL") || filename.contains("TPCH_Q1_LIMIT.SQL"))
		{
			indexCol=10;
			table='l';			
			BufferManager bm = new BufferManager(10240);
			FileManager fm = new FileManager(bm);
			if(!indexMap.containsKey("l.shipdate"))
			{
				indexMap.put("l.shipdate", "lineitem.idx");
			}			
			File sortedPath=new File("sorted_lineitemtbl.txt");
			File path=new File("lineitem.idx");
			File hashPath=new File("hashlineitem.idx");
			if(!sortedPath.exists() && !path.exists() && !hashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("lineitem.tbl", IndexTPCHFiles.getSchema("lineitem"),indexCol,table);
				int []keyCols={10};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("lineitem"), 1,keyCols);				
				//	lineItemIndex=GenerateISAMIndex(fm, path, dataSource, keySpec);
				hashlineItemIndex=GenerateHashIndex(fm,hashPath, dataSource, keySpec);				

			}



		}
		else if(filename.contains("TPCH_Q3.SQL") || filename.contains("TPCH_Q3_LIMIT.SQL"))
		{
			indexCol=10;
			table='l';			
			BufferManager bm = new BufferManager(10240);
			FileManager fm = new FileManager(bm);
			if(!indexMap.containsKey("l.shipdate"))
			{
				indexMap.put("l.shipdate", "lineitem.idx");
			}			
			File sortedPath=new File("sorted_lineitemtbl.txt");
			File path=new File("lineitem.idx");
			File hashPath=new File("hashlineitem.idx");
			if(!sortedPath.exists() || !hashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("lineitem.tbl", IndexTPCHFiles.getSchema("lineitem"),indexCol,table);
				int []keyCols={10};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("lineitem"), 1,keyCols);				
				//		lineItemIndex=GenerateISAMIndex(fm, path, dataSource, keySpec);
				hashlineItemIndex=GenerateHashIndex(fm,hashPath, dataSource, keySpec);				

			}


			indexCol=4;
			table='o';
			BufferManager obm = new BufferManager(10240);
			FileManager ofm = new FileManager(obm);
			if(!indexMap.containsKey("o.orderdate"))
			{
				indexMap.put("o.orderdate", "orders.idx");
			}					
			File osortedPath=new File("sorted_orderstbl.txt");
			File opath=new File("orders.idx");
			File ohashPath=new File("hashorders.idx");
			if(!osortedPath.exists() ||  !ohashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("orders.tbl", IndexTPCHFiles.getSchema("orders"),indexCol,table);
				int []keyCols={4};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("orders"), 1,keyCols);				
				//	ordersIndex=GenerateISAMIndex(ofm, opath, dataSource, keySpec);
				hashordersIndex=GenerateHashIndex(ofm,ohashPath, dataSource, keySpec);				

			}





			indexCol=0;
			table='c';
			BufferManager cbm = new BufferManager(10240);
			FileManager cfm = new FileManager(cbm);
			if(!indexMap.containsKey("c.custkey"))
			{
				indexMap.put("c.custkey", "customer.idx");
			}
			File csortedPath=new File("sorted_customertbl.txt");
			File cpath=new File("customer.idx");
			File chashPath=new File("hashcustomer.idx");
			if(!csortedPath.exists() || !chashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("customer.tbl", IndexTPCHFiles.getSchema("customer"),indexCol,table);		
				int keyCols[]={0};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("customer"), 1,keyCols);
				try {

					hashcustomerIndex=HashIndex.create(cfm, chashPath, dataSource, keySpec, 10240);
					//		customerIndex=ISAMIndex.create(cfm, cpath, dataSource, keySpec);
				} catch (SqlException | IOException e){e.printStackTrace();}
			}



		}
		else if(filename.contains("TPCH_Q5.SQL") || filename.contains("TPCH_Q5_LIMIT.SQL"))
		{
			indexCol=0;
			table='l';
			BufferManager bm = new BufferManager(10240);
			FileManager fm = new FileManager(bm);

			if(!indexMap.containsKey("l.orderkey"))
			{
				indexMap.put("l.orderkey", "lineitemorder.idx");
			}




			File sortedPath=new File("sorted_lineitemtblorder.txt");
			File path=new File("lineitemorder.idx");
			File hashPath=new File("hashlineitemorder.idx");
			if(!sortedPath.exists() || !hashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("lineitem.tbl", IndexTPCHFiles.getSchema("lineitem"),indexCol,table);
				int []keyCols={0};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("lineitem"), 1,keyCols);				
				//	lineItemOrdersIndex=GenerateISAMIndex(fm, path, dataSource, keySpec);
				hashlineItemOrdersIndex=GenerateHashIndex(fm,hashPath, dataSource, keySpec);				

			}





			indexCol=4;
			table='o';
			BufferManager obm = new BufferManager(10240);
			FileManager ofm = new FileManager(obm);
			if(!indexMap.containsKey("o.orderdate"))
			{
				indexMap.put("o.orderdate", "orders.idx");
			}					
			File osortedPath=new File("sorted_orderstbl.txt");
			File opath=new File("orders.idx");
			File ohashPath=new File("hashorders.idx");
			if(!osortedPath.exists() || !ohashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("orders.tbl", IndexTPCHFiles.getSchema("orders"),indexCol,table);
				int []keyCols={4};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("orders"), 1,keyCols);				
				//		ordersIndex=GenerateISAMIndex(ofm, opath, dataSource, keySpec);
				hashordersIndex=GenerateHashIndex(ofm,ohashPath, dataSource, keySpec);				

			}



			indexCol=0;
			table='c';
			BufferManager cbm = new BufferManager(10240);
			FileManager cfm = new FileManager(cbm);
			if(!indexMap.containsKey("c.custkey"))
			{
				indexMap.put("c.custkey", "customer.idx");
			}
			File csortedPath=new File("sorted_customertbl.txt");
			File cpath=new File("customer.idx");
			File chashPath=new File("hashcustomer.idx");
			if(!csortedPath.exists() || !chashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("customer.tbl", IndexTPCHFiles.getSchema("customer"),indexCol,table);		
				int keyCols[]={0};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("customer"), 1,keyCols);
				try {

					hashcustomerIndex=HashIndex.create(cfm, chashPath, dataSource, keySpec, 10240);
					//				customerIndex=ISAMIndex.create(cfm, cpath, dataSource, keySpec);
				} catch (SqlException | IOException e){e.printStackTrace();}
			}



			indexCol=0;
			table='s';
			BufferManager sbm = new BufferManager(10240);
			FileManager sfm = new FileManager(sbm);
			if(!indexMap.containsKey("s.suppkey"))
			{
				indexMap.put("s.suppkey", "supplier.idx");
			}
			File ssortedPath=new File("sorted_suppliertbl.txt");
			File spath=new File("supplier.idx");
			File shashPath=new File("hashsupplier.idx");


			if(!ssortedPath.exists() || !shashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("supplier.tbl", IndexTPCHFiles.getSchema("supplier"),indexCol,table);
				int keyCols[]={0};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("supplier"), 1,keyCols);
				try {

					hashsupplierIndex=HashIndex.create(sfm, shashPath, dataSource, keySpec, 10240);
					//			supplierIndex=ISAMIndex.create(sfm, spath, dataSource, keySpec);
				} catch (SqlException | IOException e){e.printStackTrace();}
			}



			indexCol=0;
			table='n';
			BufferManager nbm = new BufferManager(10240);
			FileManager nfm = new FileManager(nbm);
			if(!indexMap.containsKey("n.nationkey"))
			{
				indexMap.put("n.nationkey", "nation.idx");
			}

			File nsortedPath=new File("sorted_nationtbl.txt");
			File npath=new File("nation.idx");
			File nhashPath=new File("hashnation.idx");


			if(!nsortedPath.exists() || !nhashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("nation.tbl", IndexTPCHFiles.getSchema("nation"),indexCol,table);
				int keyCols[]={0};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("nation"),keyCols);
				try {

					hashnationIndex=HashIndex.create(nfm, nhashPath, dataSource, keySpec, 10240);
					//			nationIndex=ISAMIndex.create(nfm, npath, dataSource, keySpec);
				} catch (SqlException | IOException e){e.printStackTrace();}
			}








			indexCol=0;
			table='r';
			BufferManager rbm = new BufferManager(10240);
			FileManager rfm = new FileManager(rbm);

			if(!indexMap.containsKey("r.name"))
			{
				indexMap.put("r.name", "region.idx");
			}
			File rsortedPath=new File("sorted_regiontbl.txt");
			File rpath=new File("region.idx");
			File rhashPath=new File("hashregion.idx");



			if(!rsortedPath.exists() || !rhashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("region.tbl", IndexTPCHFiles.getSchema("region"),indexCol,table);
				int keyCols[]={0};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("region"), 1,keyCols);
				try {

					hashregionIndex=HashIndex.create(rfm, rhashPath, dataSource, keySpec, 10240);
					//				regionIndex=ISAMIndex.create(rfm, rpath, dataSource, keySpec);
				} catch (SqlException | IOException e){e.printStackTrace();}
			}



		}
		else if(filename.contains("TPCH_Q6.SQL"))
		{

			indexCol=10;
			table='l';			
			BufferManager bm = new BufferManager(10240);
			FileManager fm = new FileManager(bm);
			if(!indexMap.containsKey("l.shipdate"))
			{
				indexMap.put("l.shipdate", "lineitem.idx");
			}			
			File sortedPath=new File("sorted_lineitemtbl.txt");
			File path=new File("lineitem.idx");
			File hashPath=new File("hashlineitem.idx");
			if(!sortedPath.exists() || !hashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("lineitem.tbl", IndexTPCHFiles.getSchema("lineitem"),indexCol,table);
				int []keyCols={10};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("lineitem"), 1,keyCols);				
				//	lineItemIndex=GenerateISAMIndex(fm, path, dataSource, keySpec);
				hashlineItemIndex=GenerateHashIndex(fm,hashPath, dataSource, keySpec);				

			}



		}
		else if(filename.contains("TPCH_Q10.SQL") || filename.contains("TPCH_Q10_LIMIT.SQL"))
		{

			indexCol=0;
			table='c';
			BufferManager cbm = new BufferManager(10240);
			FileManager cfm = new FileManager(cbm);
			if(!indexMap.containsKey("c.custkey"))
			{
				indexMap.put("c.custkey", "customer.idx");
			}
			File csortedPath=new File("sorted_customertbl.txt");
			File cpath=new File("customer.idx");
			File chashPath=new File("hashcustomer.idx");
			if(!csortedPath.exists() || !chashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("customer.tbl", IndexTPCHFiles.getSchema("customer"),indexCol,table);		
				int keyCols[]={0};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("customer"), 1,keyCols);
				try {

					hashcustomerIndex=HashIndex.create(cfm, chashPath, dataSource, keySpec, 10240);
					//					customerIndex=ISAMIndex.create(cfm, cpath, dataSource, keySpec);
				} catch (SqlException | IOException e){e.printStackTrace();}
			}





			indexCol=4;
			table='o';
			BufferManager obm = new BufferManager(10240);
			FileManager ofm = new FileManager(obm);
			if(!indexMap.containsKey("o.orderdate"))
			{
				indexMap.put("o.orderdate", "orders.idx");
			}					
			File osortedPath=new File("sorted_orderstbl.txt");
			File opath=new File("orders.idx");
			File ohashPath=new File("hashorders.idx");
			if(!osortedPath.exists() || !ohashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("orders.tbl", IndexTPCHFiles.getSchema("orders"),indexCol,table);
				int []keyCols={4};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("orders"), 1,keyCols);				
				//			ordersIndex=GenerateISAMIndex(ofm, opath, dataSource, keySpec);
				hashordersIndex=GenerateHashIndex(ofm,ohashPath, dataSource, keySpec);				

			}


			indexCol=0;
			table='l';
			BufferManager bm = new BufferManager(10240);
			FileManager fm = new FileManager(bm);

			if(!indexMap.containsKey("l.orderkey"))
			{
				indexMap.put("l.orderkey", "lineitemorder.idx");
			}




			File sortedPath=new File("sorted_lineitemtblorder.txt");
			File path=new File("lineitemorder.idx");
			File hashPath=new File("hashlineitemorder.idx");
			if(!sortedPath.exists() || !hashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("lineitem.tbl", IndexTPCHFiles.getSchema("lineitem"),indexCol,table);
				int []keyCols={0};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("lineitem"), 1,keyCols);				
				//			lineItemIndex=GenerateISAMIndex(fm, path, dataSource, keySpec);
				hashlineItemIndex=GenerateHashIndex(fm,hashPath, dataSource, keySpec);				

			}



			indexCol=0;
			table='n';
			BufferManager nbm = new BufferManager(10240);
			FileManager nfm = new FileManager(nbm);
			if(!indexMap.containsKey("n.nationkey"))
			{
				indexMap.put("n.nationkey", "nation.idx");
			}

			File nsortedPath=new File("sorted_nationtbl.txt");
			File npath=new File("nation.idx");
			File nhashPath=new File("hashnation.idx");


			if(!nsortedPath.exists() || !nhashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("nation.tbl", IndexTPCHFiles.getSchema("nation"),indexCol,table);
				int keyCols[]={0};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("nation"),keyCols);
				try {

					hashnationIndex=HashIndex.create(nfm, nhashPath, dataSource, keySpec, 10240);
					//				nationIndex=ISAMIndex.create(nfm, npath, dataSource, keySpec);
				} catch (SqlException | IOException e){e.printStackTrace();}
			}




		}
		else if(filename.contains("TPCH_Q19.SQL"))
		{


			indexCol=4;
			table='l';		
			BufferManager bm = new BufferManager(10240);
			FileManager fm = new FileManager(bm);
			if(!indexMap.containsKey("l.quantity"))
			{
				indexMap.put("l.quantity", "lineitemquant.idx");
			}		
			File sortedPath=new File("sorted_lineitemtblquant.txt");
			File path=new File("lineitemquant.idx");
			File hashPath=new File("hashlineitemquant.idx");
			if(!sortedPath.exists() || !hashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("lineitem.tbl", IndexTPCHFiles.getSchema("lineitem"),indexCol,table);
				int []keyCols={4};
				IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("lineitem"), 1,keyCols);				
				//			lineItemQuantIndex =GenerateISAMIndex(fm, path, dataSource, keySpec);
				hashlineItemQuantIndex=GenerateHashIndex(fm,hashPath, dataSource, keySpec);				

			}


			indexCol=6;
			table='p';	
			BufferManager pbm = new BufferManager(10240);
			FileManager pfm = new FileManager(pbm);
			if(!indexMap.containsKey("p.container"))
			{
				indexMap.put("p.container", "part.idx");
			}	
			File psortedPath=new File("sorted_parttbl.txt");
			File ppath=new File("part.idx");
			File phashPath=new File("hashlineitempart.idx");
			if(!psortedPath.exists() || !phashPath.exists())
			{
				Iterator<Datum[]> dataSource = getFileContents("part.tbl", IndexTPCHFiles.getSchema("part"),indexCol,table);
				int []keyCols={6};
				final IndexKeySpec keySpec = new GenericIndexKeySpec(IndexTPCHFiles.getSchema("part"), 1,keyCols);
				//				partIndex=GenerateISAMIndex(pfm, ppath, dataSource, keySpec);
				hashpartIndex=GenerateHashIndex(pfm,phashPath, dataSource, keySpec);				

			}




		}

	}

	public static HashIndex GenerateHashIndex(FileManager fm,File path,Iterator<Datum[]> dataSource,IndexKeySpec keySpec) 	
	{
		HashIndex hashIndex=null;
		if(!path.exists())
		{
			try
			{				
				hashIndex=HashIndex.create(fm, path, dataSource, keySpec,10240);
				System.out.println("HASH Index created for: "+path);
			}
			catch(Exception e)
			{
				System.out.println("Index creation failed!\n"+e);
				e.printStackTrace();
			}

		}
		return hashIndex;


	}

	public static ISAMIndex GenerateISAMIndex(FileManager fm,File path,Iterator<Datum[]> dataSource,IndexKeySpec keySpec) 	
	{
		ISAMIndex sampleIndex = null;

		if(!path.exists())
		{
			try
			{
				sampleIndex=ISAMIndex.create(fm, path, dataSource, keySpec);
				System.out.println("Isam Index created for: "+path);
			}
			catch(Exception e)
			{
				System.out.println("Index creation failed!\n"+e);
				e.printStackTrace();
			}

		}
		return sampleIndex;
	}

	public static Iterator<Datum[]> getFileContents(String fileName, Schema.Type[] sch,int indexCol,char table) 
	{
		ArrayList<Datum[]> tableDatum=new ArrayList<Datum[]>();	

		try
		{
			System.out.println("file name is "+fileName);
			BufferedReader br=new BufferedReader(new FileReader(fileName));		
			fileName=fileName.substring(fileName.indexOf("/")+1,fileName.length());
			String line;
			int col=0;
			int noOfCol=0;

			while((line=br.readLine()) != null)
			{
				String splits[]=line.split("\\|");
				noOfCol=splits.length;
				Datum []d=new Datum[noOfCol];
				for(col=0;col<noOfCol;col++)
				{	
					splits[col]=splits[col].replace("-", "");
					d[col]=GetTableContents.Builddatum(sch[col],splits[col]);

				}		
				tableDatum.add(d);
			}

			if(indexCol!=0)
				Collections.sort(tableDatum,new Sorttpchtable(indexCol,table));


			fileName=fileName.replace(".", "");
			FileWriter fileWriter=null;
			if(table=='l' && indexCol==0 )
			{
				fileWriter=new FileWriter(new File("sorted_"+fileName+"order.txt"));
			}
			else if(table=='l' && indexCol==4 )
			{
				fileWriter=new FileWriter(new File("sorted_"+fileName+"quant.txt"));
			}
			else
			{	
				fileWriter=new FileWriter(new File("sorted_"+fileName+".txt"));
			}
			for(int i=0; i<tableDatum.size(); i++)
			{
				for(int j=0; j<tableDatum.get(i).length; j++)
				{
					fileWriter.write(tableDatum.get(i)[j]+" | ");
				}
				fileWriter.write("\n");
			}
			fileWriter.flush();
			fileWriter.close();
		}
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		catch(Exception e)
		{
			System.out.println("Exception: "+e);
			e.printStackTrace();
		}

		return tableDatum.iterator();
	}
}