package edu.buffalo.cse.sql.index;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.test.TestDataStream;

public class ISAMIndex implements IndexFile
{

	//global declaration
	public ManagedFile managedFile;
	public IndexKeySpec keySpec;
	public IndexKeySpec indexPageKeySpec;
	public ISAMIndex(ManagedFile file, IndexKeySpec keySpec) throws IOException, SqlException
	{
		this.keySpec=keySpec;
		this.managedFile=file;
		
		Schema.Type[] indexPageSchema=new Schema.Type[keySpec.rowSchema().length];
		for(int i=0; i<keySpec.rowSchema().length; i++)
			indexPageSchema[i]=Schema.Type.INT;
		indexPageKeySpec=new GenericIndexKeySpec(indexPageSchema, 1);
		
		

	}
	public static ISAMIndex create(FileManager fm, File path, Iterator<Datum[]> dataSource,	IndexKeySpec key)	throws SqlException, IOException
	{

		int value;
		int totalPageCount=0;
		int dp=0;
		int indexPagePointer;

		

		Datum[] row1;
		Datum[] keys1;
		DatumBuffer datumbuf;

		ByteBuffer bytebuf;
		ByteBuffer databuf; 
		int managedSize=1024;
		int indexPages=0;

		//FileWriter fw1=new FileWriter(new File("metadata.txt"));

		ManagedFile mf=fm.open(path);

		mf.resize(0);
		mf.ensureSize(managedSize);

		//TestDataStream td=(TestDataStream)dataSource;


		//System.out.println("Number of attributes in the schema: "+key.rowSchema().length);
		
		Schema.Type[] indexPageSchema=new Schema.Type[key.rowSchema().length];
		for(int i=0; i<key.rowSchema().length; i++)
			indexPageSchema[i]=Schema.Type.INT;
		IndexKeySpec indexPageKeySpec=new GenericIndexKeySpec(indexPageSchema, 1);
		
		
		
		int dataPages=0;
		HashMap<Integer,Boolean> seen=new HashMap<Integer,Boolean>();


		while(dataSource.hasNext())  
		{
			

			Datum[] data=dataSource.next();
			try
			{
				if(totalPageCount==managedSize-1)
				{
					mf.ensureSizeByDoubling(managedSize+1);
					managedSize=mf.size();
				}                        
				ByteBuffer bf1=mf.pin(totalPageCount);
				DatumBuffer dbuf=new DatumBuffer(bf1,null);
				if(seen.containsKey(totalPageCount)==false)
				{
					//System.out.println("Datapage: "+totalPageCount);
					dbuf.initialize(5);
					seen.put(totalPageCount, true);
				}                                
				if(dbuf.remaining()-8<DatumSerialization.getLength(data))
				{
					
					

					mf.unpin(totalPageCount,true);

					totalPageCount=totalPageCount+1;
					ByteBuffer tempBuf=mf.pin(totalPageCount);
					DatumBuffer tempDatum=new DatumBuffer(tempBuf,null);
					if(!seen.containsKey(totalPageCount))
					{
						//System.out.println("Datapage: "+totalPageCount);
						tempDatum.initialize(5);        
						seen.put(totalPageCount, true);
					}
					value=tempDatum.write(data);
					tempBuf.putInt(0,value);

					mf.unpin(totalPageCount,true);
				}
				else
				{
					value=dbuf.write(data);
					bf1.putInt(0,value);

					mf.unpin(totalPageCount,true);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		
		//mf.flush();

		totalPageCount++;
		dataPages=totalPageCount;

		//System.out.println("No of pages containing data: "+(dataPages));
		
		indexPagePointer=totalPageCount;
		bytebuf=mf.safePin(indexPagePointer);

		datumbuf=new DatumBuffer(bytebuf,key.keySchema());
		if(!seen.containsKey(indexPagePointer))
		{
			datumbuf.initialize(5);
			seen.put(indexPagePointer, true);
		}
		value=datumbuf.write(new Datum[]{new Datum.Int(0)});
		bytebuf.putInt(0,value);
		mf.unpin(indexPagePointer,true);
		
		seen.clear(); // change made here

		for(dp=1; dp<dataPages; dp++)
		{
			if(totalPageCount==managedSize-1)
			{
				mf.ensureSizeByDoubling(managedSize+1);
				managedSize=mf.size();
			}      
			bytebuf=mf.safePin(indexPagePointer);
			datumbuf=new DatumBuffer(bytebuf, null);

			if(!seen.containsKey(indexPagePointer))
			{
				//System.out.println("Index Page Level 1: "+totalPageCount);
				datumbuf.initialize(5);
				seen.put(indexPagePointer, true);
				value=datumbuf.write(new Datum[]{new Datum.Int(dp-1)});
				bytebuf.putInt(0,value);
			}
			try
			{
				databuf=mf.pin(dp);
				DatumBuffer datapage=new DatumBuffer(databuf,key.keySchema());
				row1=datapage.read(0);
				mf.unpin(dp);
				keys1= key.createKey(row1);
				int dataSize=DatumSerialization.getLength(keys1)+DatumSerialization.getLength(new Datum[]{new Datum.Int(dp)});
				if (datumbuf.remaining()-8>=dataSize)
				{
					value=datumbuf.write(keys1);
					bytebuf.putInt(0,value);
					value=datumbuf.write(new Datum[]{new Datum.Int(dp)});
					bytebuf.putInt(0,value);
					mf.unpin(indexPagePointer,true);
				}
				else
				{
					
					mf.unpin(indexPagePointer,true);
					indexPagePointer++;
					indexPages++;
					totalPageCount++;					
				}
			}
			catch(InsufficientSpaceException e)
			{
				System.out.println("Insufficient space exception in level 1");
				mf.unpin(indexPagePointer,true);
				indexPagePointer++;
				indexPages++;
				totalPageCount++;
			}
			
		}
		//mf.flush();
		seen.clear();
		
		indexPages++;
		totalPageCount++;

		//System.out.println("Number of Index Nodes in level 1: "+(indexPages));
		

		int numberOfIndexPages=indexPages;
		int levelPageCount=0;

		int start=totalPageCount-indexPages;
		int end=start+indexPages-1;
		int level=1;
		
		
		while(numberOfIndexPages>1)
		{
			
			if(totalPageCount==managedSize-1)
			{
				mf.ensureSizeByDoubling(managedSize+1);
				managedSize=mf.size();
			}      


			bytebuf=mf.safePin(totalPageCount);
			datumbuf=new DatumBuffer(bytebuf,null);
			if(!seen.containsKey(totalPageCount))
			{
				datumbuf.initialize(5);
				seen.put(totalPageCount, true);
			}

			value=datumbuf.write(new Datum[]{new Datum.Int(start)});
			bytebuf.putInt(0,value);
			mf.unpin(totalPageCount,true);

			levelPageCount=0;


			for(int indexPage=start+1; indexPage<=end; indexPage++)
			{
				if(totalPageCount==managedSize-1)
				{
					mf.ensureSizeByDoubling(managedSize+1);
					managedSize=mf.size();
				}      

				bytebuf=mf.safePin(totalPageCount);
				datumbuf=new DatumBuffer(bytebuf, null);
				if(!seen.containsKey(totalPageCount))
				{
					datumbuf.initialize(5);
					seen.put(totalPageCount, true);

					value=datumbuf.write(new Datum[]{new Datum.Int(indexPage-1)});
					bytebuf.putInt(0,value);

				}
				try
				{
					databuf=mf.pin(indexPage);
					DatumBuffer datapage=new DatumBuffer(databuf,indexPageKeySpec.rowSchema());
					//System.out.println("ISAMIndex>Page length: "+datapage.length());
					row1=datapage.read(0);
					mf.unpin(indexPage);

					Datum[] row2=null;

					for(int g=1; g<level+1; g++)
					{
						ByteBuffer page2=mf.pin(row1[0].toInt());
						DatumBuffer datapage2=new DatumBuffer(page2,indexPageKeySpec.rowSchema());
						row2=datapage2.read(0);

						mf.unpin(row1[0].toInt());
						row1=row2;
					}
					Datum[] key2=key.createKey(row2);
					int dataSize=DatumSerialization.getLength(row1)+DatumSerialization.getLength(new Datum[]{new Datum.Int(indexPage)});

					if (datumbuf.remaining()-8>=dataSize)
					{
						value=datumbuf.write(key2);
						bytebuf.putInt(0,value);

						value=datumbuf.write(new Datum[]{new Datum.Int(indexPage)});
						bytebuf.putInt(0,value);

						mf.unpin(totalPageCount,true);
					}
					else
					{
						//System.out.println("Index Page Level "+level+": "+totalPageCount);
						mf.unpin(totalPageCount,true);
						levelPageCount++;
						totalPageCount++;
						//System.out.println("Total Page Count Incremented: "+totalPageCount);
						indexPage-=1;
						//System.out.println("IndexPage Decremented: "+indexPage);
					}

				}
				catch(InsufficientSpaceException e)
				{
					System.out.println("Algorithm: Index Page is full! Catch Block");
					mf.unpin(totalPageCount,true);
					totalPageCount++;
					levelPageCount++;
				}
				
				seen.clear();
				
			}
			
			totalPageCount++;
			levelPageCount++;
			start=totalPageCount-levelPageCount;
			end=totalPageCount-1;
			numberOfIndexPages=end-start+1;

			//mf.flush();
			level++;
			//System.out.println("Number of Index Nodes in level "+level+": "+(numberOfIndexPages));
			
		}
		//System.out.println("Total number of pages in the file: "+(totalPageCount));
		//System.out.println("Total number of levels in the isam structure: "+level);

		
		ByteBuffer metaPage=mf.safePin(totalPageCount);
		DatumBuffer writePage=new DatumBuffer(metaPage,null);
		
		writePage.initialize(5);
		value=writePage.write(new Datum[]{new Datum.Int(level)});
		metaPage.putInt(0,value);
		
		value=writePage.write(new Datum[]{new Datum.Int(dataPages)});
		metaPage.putInt(0,value);
		
		mf.unpin(totalPageCount,true);
		mf.flush();
		
		totalPageCount++;
		
		System.out.println("Total number of pages in the file after writing the metapage: "+(totalPageCount));
		
		fm.close(path);
		System.out.println("Create Successful for : "+path);
		return new ISAMIndex(mf, key);
	}

	public IndexIterator scan() throws SqlException, IOException
	{
		DatumStreamIterator dsItr = new DatumStreamIterator(managedFile, keySpec.rowSchema());		
		int metaPageNumber=managedFile.size()-1;
		ByteBuffer readPage=managedFile.pin(metaPageNumber);
		DatumBuffer dataPage=new DatumBuffer(readPage,keySpec.rowSchema());		
		int dataPages=dataPage.read(1)[0].toInt();		
		managedFile.unpin(metaPageNumber);
		dsItr.currPage(0);
		dsItr.key(keySpec);
		dsItr.maxPage(dataPages-1);

		System.out.println("no"+dataPages);
		dsItr.ready();

		return dsItr;
	}

	public IndexIterator rangeScanTo(Datum[] toKey)throws SqlException, IOException
	{
		DatumStreamIterator dsItr = new DatumStreamIterator(managedFile, keySpec.rowSchema());
		dsItr.key(keySpec);
		Datum[] upto=getLeafPage(toKey);
		int i;
		Datum[] row=null;
		Datum[] data=null;
		dsItr.currPage(0);
		
		ByteBuffer readPage=managedFile.pin(upto[0].toInt());
		DatumBuffer dataPage=new DatumBuffer(readPage,keySpec.rowSchema());
		for(i=0; i<=readPage.getInt(0); i++)
		{

			row=dataPage.read(i);
			data=keySpec.createKey(row);
			if(Datum.compareRows(toKey,data)>=0)
			{
				break;
			}
		}
		while(Datum.compareRows(toKey,data)>=0 )
		{
			row=dataPage.read(++i);
			data=keySpec.createKey(row);
		}
		dsItr.maxPage(upto[0].toInt());
		dsItr.maxRecord(row);
		dsItr.ready();
		return dsItr;
	}


	public Datum[] getLeafPage(Datum[] key) throws SqlException, IOException
	{
		int pageIndex=1;
		int searchPage=-1;
		int pageLength=-1;
		int rootPageNumber=managedFile.size()-2;
		int newSearchPage=-1;
		
		int metaPageNumber=managedFile.size()-1;
		ByteBuffer readPage=managedFile.pin(metaPageNumber);
		DatumBuffer dataPage=new DatumBuffer(readPage,keySpec.rowSchema());
		int levels=dataPage.read(0)[0].toInt();		
		managedFile.unpin(metaPageNumber);
		
		int keySearch=key[0].toInt();
		searchPage=rootPageNumber;
		int flag=0;
		
		
		while(levels>0 && flag==0)
		{
			newSearchPage=-1;
			readPage=managedFile.pin(searchPage);
			dataPage=new DatumBuffer(readPage,keySpec.rowSchema());
			pageLength=readPage.getInt(0)+1;
		
			
			for(pageIndex=1; pageIndex<pageLength; pageIndex=pageIndex+2)
			{

				Datum[] data=dataPage.read(pageIndex);
				int keyValue=data[0].toInt();
				int keyValue2;
				Datum[] data2;

				if(pageIndex==pageLength-2 && keySearch>keyValue)
				{
					data=dataPage.read(pageIndex+1);
					newSearchPage=data[0].toInt();
					break;
				}
				if(keySearch<keyValue)
				{
					
					data=dataPage.read(pageIndex-1);
					newSearchPage=data[0].toInt();
					break;
				}
				else if(keySearch==keyValue)
				{
					data=dataPage.read(pageIndex+1);
					newSearchPage=data[0].toInt();
					flag=1;
					break;
				}
				else
				{
					data=dataPage.read(pageIndex);
					keyValue=data[0].toInt();
					data2=dataPage.read(pageIndex+2);
					keyValue2=data2[0].toInt();
					if(keySearch>keyValue && keySearch <keyValue2)
					{
						data=dataPage.read(pageIndex+1);
						newSearchPage=data[0].toInt();
						break;

					}
				}
			}
			
			managedFile.unpin(searchPage);
			levels--;
			if(newSearchPage!=-1)
				searchPage=newSearchPage;
		}
		readPage=managedFile.pin(searchPage);
		dataPage=new DatumBuffer(readPage,keySpec.rowSchema());
		int rowID=dataPage.find(key);
		managedFile.unpin(searchPage);
		Datum data[]=new Datum[]{new Datum.Int(searchPage),new Datum.Int(rowID)};
		return data;
	}

	public IndexIterator rangeScanFrom(Datum[] fromKey)	throws SqlException, IOException
	{
		DatumStreamIterator dsItr = new DatumStreamIterator(managedFile, keySpec.rowSchema());
		
		int metaPageNumber=managedFile.size()-1;
		ByteBuffer readPage=managedFile.pin(metaPageNumber);
		DatumBuffer dataPage=new DatumBuffer(readPage,keySpec.rowSchema());
		
		int dataPages=dataPage.read(1)[0].toInt();
		
		managedFile.unpin(metaPageNumber);

		dsItr.key(keySpec);
		Datum[] firstPage=getLeafPage(fromKey);

		readPage=managedFile.pin(firstPage[0].toInt());
		dataPage=new DatumBuffer(readPage,keySpec.rowSchema());

		int i;
		int rowID=firstPage[1].toInt();

		for(i=0; i<readPage.getInt(0); i++)
		{
			Datum[] data=dataPage.read(i);
			data=keySpec.createKey(data);
			if(Datum.compareRows(data,fromKey)>=0)
			{
				break;
			}
		}
		dsItr.currPage(firstPage[0].toInt());
		dsItr.currRecord(i);
		dsItr.maxPage(dataPages-1);
		dsItr.ready();
		return dsItr;
	}

	public IndexIterator rangeScan(Datum[] start, Datum[] end) throws SqlException, IOException
	{
		DatumStreamIterator dsItr = new DatumStreamIterator(managedFile, keySpec.rowSchema());
		
		dsItr.key(keySpec);
		Datum[] upto=getLeafPage(end);
		Datum[] firstPage=getLeafPage(start);
		ByteBuffer readPage=managedFile.pin(firstPage[0].toInt());
		DatumBuffer dataPage=new DatumBuffer(readPage,keySpec.rowSchema());
		int i;
		Datum[] row=null;
		Datum[] data=null;

		for(i=0; i<=readPage.getInt(0); i++)
		{

			data=dataPage.read(i);
			data=keySpec.createKey(data);
			if(Datum.compareRows(data,start)>=0)
			{
				break;
			}
		}
		
		dsItr.currPage(firstPage[0].toInt());
		dsItr.currRecord(i);

		readPage=managedFile.pin(upto[0].toInt());
		dataPage=new DatumBuffer(readPage,keySpec.rowSchema());

		for(i=0; i<=readPage.getInt(0); i++)
		{

			row=dataPage.read(i);
			data=keySpec.createKey(row);
			if(Datum.compareRows(end,data)>=0)
			{
				break;
			}
		}
		while(Datum.compareRows(end,data)>=0)
		{
			row=dataPage.read(++i);
			data=keySpec.createKey(row);
		}
		
		dsItr.maxPage(upto[0].toInt());
		dsItr.maxRecord(row);
		dsItr.ready();
		return dsItr;
	}
	public Datum[] get(Datum[] key) throws SqlException, IOException
	{
		int pageIndex=1;
		int searchPage=-1;
		int pageLength=-1;
		int rootPageNumber=managedFile.size()-2;
		int metaPageNumber=managedFile.size()-1;
		
		
		int newSearchPage=-1;
		
		
		
		
		
		ByteBuffer readPage=managedFile.pin(metaPageNumber);
		DatumBuffer dataPage=new DatumBuffer(readPage,indexPageKeySpec.rowSchema());
		
		
		
		int levels=dataPage.read(0)[0].toInt();
		
		
		
		managedFile.unpin(metaPageNumber);
		
		
	
		int keySearch=key[0].toInt();

		searchPage=rootPageNumber;
		int flag=0;

				

		while(levels>0 && flag==0)
		{
			newSearchPage=-1;
			readPage=managedFile.pin(searchPage);
			dataPage=new DatumBuffer(readPage,indexPageKeySpec.rowSchema());
			
			pageLength=readPage.getInt(0)+1;
			
			for(pageIndex=1; pageIndex<pageLength; pageIndex=pageIndex+2)
			{

				Datum[] data=dataPage.read(pageIndex);
				int keyValue=data[0].toInt();
				int keyValue2;
				Datum[] data2;

				if(pageIndex==pageLength-2 && keySearch>keyValue)
				{
					data=dataPage.read(pageIndex+1);
					newSearchPage=data[0].toInt();
					break;

				}
				if(keySearch<keyValue)
				{
					
					data=dataPage.read(pageIndex-1);
					newSearchPage=data[0].toInt();
					break;
				}
				else if(keySearch==keyValue)
				{
					
					data=dataPage.read(pageIndex+1);
					newSearchPage=data[0].toInt();
					flag=1;
					break;
				}
				else
				{
					data=dataPage.read(pageIndex);
					keyValue=data[0].toInt();
					data2=dataPage.read(pageIndex+2);
					keyValue2=data2[0].toInt();
					if(keySearch>keyValue && keySearch <keyValue2)
					{
						data=dataPage.read(pageIndex+1);
						newSearchPage=data[0].toInt();
						break;
					}
				}
			}
			managedFile.unpin(searchPage);
			levels--;
			if(newSearchPage!=-1)
				searchPage=newSearchPage;
		}

		readPage=managedFile.pin(searchPage);
		dataPage=new DatumBuffer(readPage,keySpec.rowSchema());

		int rowID=dataPage.find(key);
		Datum[] found=dataPage.read(rowID);
		Datum[] foundKey=keySpec.createKey(found);
		
		managedFile.unpin(searchPage);
		if(Datum.compareRows(foundKey, key)==0)
		{
			System.out.println("Keys are same");
			return found;
		}
		else
		{
			System.out.println("Keys Dont Match");
			return null;
		}

	}
}