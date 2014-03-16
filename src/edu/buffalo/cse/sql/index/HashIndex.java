package edu.buffalo.cse.sql.index;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;

public class HashIndex implements IndexFile 
{
	static ManagedFile file;
	static IndexKeySpec keySpec;
	FileManager fm;
	File path;
	Iterator<Datum[]> dataSource;
	static int dirSize;
	public HashIndex(ManagedFile file, IndexKeySpec keySpec) throws IOException, SqlException
	{
		this.file=file;
		this.keySpec=keySpec;
	}

	public static HashIndex create(FileManager fm,File path,Iterator<Datum[]> dataSource,IndexKeySpec key,int directorySize)throws SqlException, IOException
	{

		int sizeBeforeOverflow=0;
		/*------------------saving the directory info
		try
		{
			FileWriter fstream = new FileWriter("dirInfo.txt");
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(directorySize+"");
			out.close();
		} catch (Exception e2){e2.printStackTrace();}
		System.out.println();
		//---------------------------------*/

		int modifiedDir=directorySize;
		int latestOfPage=0;
		ManagedFile manFile=fm.open(path);
		manFile.resize(0);
		manFile.ensureSize(directorySize);
		HashMap<Integer,DatumBuffer> seen=new HashMap<Integer,DatumBuffer>();
		HashMap<Integer,Boolean> setOverflow=new HashMap<Integer, Boolean>();
		HashMap<Integer,Integer> trackOf=new HashMap<Integer, Integer>();
		int count = 0;
		File logFile=new File("log.txt");
		if(logFile.exists())
		{
			logFile.delete();
		}
		//FileWriter fw=new FileWriter(logFile);
		while(dataSource.hasNext())
		{
			count++;
			Datum d[]=dataSource.next();

			/*for(int k=0; k<d.length; k++)
				fw.write(d[k]+"-");
			fw.write("\n");
			 */


			int k=0;
			Datum keyDatum[]=key.createKey(d);
			int index=key.hashKey(keyDatum)%directorySize;
			/*if(index==3)
			{
				//	System.out.println("indexing at 3");
				for(k=0; k<d.length; k++)
					fw.write(d[k]+"-");
				fw.write("\n");
			}*/
			ByteBuffer by=manFile.pin(index);
			DatumBuffer dBuf=null;

			try 
			{
				if(seen.containsKey(index))
				{
					dBuf=new DatumBuffer(by,null);
				}
				else
				{
					dBuf=new DatumBuffer(by,null);	
					dBuf.initialize(5);
					by.putInt(0, -1);
					seen.put(index, dBuf);
				}
			} catch (Exception e1) {e1.printStackTrace();}


			if(setOverflow.containsKey(index))
			{

				int ovrflwindex=0;
				if(trackOf.containsKey(index))
				{
					ovrflwindex=trackOf.get(index);
				}
				ByteBuffer checkBuf=manFile.pin(ovrflwindex);
				DatumBuffer ovrflwBuf=new DatumBuffer(checkBuf,null);
				if(ovrflwBuf.remaining()-8<DatumSerialization.getLength(d))
				{
					manFile.unpin(ovrflwindex);//unpin overflowbuffer											
					modifiedDir++;
					latestOfPage=modifiedDir;
					manFile.ensureSizeByDoubling(modifiedDir+1);	
					ByteBuffer modByte=manFile.pin(latestOfPage);
					DatumBuffer modBuffer=null;
					if(!seen.containsKey(latestOfPage))
					{
						modBuffer=new DatumBuffer(modByte,null);
						modByte.putInt(0, -1);
						modBuffer.initialize(5);						
						seen.put(latestOfPage, modBuffer);
					}
					trackOf.put(index,latestOfPage);
					checkBuf.putInt(0,latestOfPage);
					modBuffer.write(d);
					manFile.unpin(latestOfPage, true);

					//++++++++++++++updating hashmap starts++++++++++++

					trackOf.remove(index);
					trackOf.put(index,latestOfPage);

					//++++++++++++++++++updating hashmap ends ++++++

				}
				else
				{

					//	System.out.println("-------------NO CHAIN PRESENT  ------------------------->");
					ovrflwBuf.write(d);
					manFile.unpin(ovrflwindex, true);
				}

			}
			else
			{
				try
				{
					dBuf.write(d);
					manFile.unpin(index,true);
				}catch(Exception e )
				{
					sizeBeforeOverflow=modifiedDir;
					modifiedDir++;
					manFile.ensureSizeByDoubling(modifiedDir+1);
					//	System.out.println("managed file doubled ");
					latestOfPage=modifiedDir;
					setOverflow.put(index, true);
					trackOf.put(index,modifiedDir);
					ByteBuffer ovrflwbyteBuf=manFile.pin(latestOfPage);
					DatumBuffer ovrflwbuf =new DatumBuffer(ovrflwbyteBuf, null);
					by.putInt(0,latestOfPage);
					ovrflwbyteBuf.putInt(0, -1);
					manFile.unpin(latestOfPage,true);

				}


			}

			//	System.out.println("+++++++++ends++++++++++++\n ");



		}
	//	fw.flush();
		manFile.flush();

		return null;

	}


	public static ArrayList<Datum[]> multget(Datum[] key,IndexKeySpec keySpec,FileManager fm, File path,int dirSize) throws SqlException, IOException
	{


		ManagedFile mFile=fm.open(path);
		
		//	System.out.println(key[0]);
		int index=keySpec.hashKey(key)%dirSize;
				//System.out.println("searching for the record at index "+index);
		ByteBuffer buf=mFile.getBuffer(index);
		DatumBuffer db=new DatumBuffer(buf, keySpec.rowSchema());
		int chkForOvrFlw=buf.getInt(0);

		ArrayList<Datum[]>results=new ArrayList<Datum[]>();
		if(chkForOvrFlw==-1) //no overflow pages present
		{
			ByteBuffer by=mFile.getBuffer(index);
			DatumBuffer searchDatum=new DatumBuffer(by, keySpec.rowSchema());
			for(int tuples=0;tuples<searchDatum.length();tuples++)
			{
				Datum[] tempresult=searchDatum.read(tuples);
				if(Datum.compareRows(keySpec.createKey(tempresult),key)==0)
				{
					//	System.out.println("keys are equal");
					
					results.add(tempresult);

				}	
			}
			return results;
		}
		else
		{
			int ovrFlwpg=buf.getInt(0);
			ArrayList<Integer> ofIndex=new ArrayList<Integer>(); 
			ofIndex.add(index);
			while(ovrFlwpg!=-1)
			{
				ofIndex.add(ovrFlwpg);
				ByteBuffer by=mFile.getBuffer(ovrFlwpg);
				ovrFlwpg=by.getInt(0);
			}
			//		System.out.println("--------------------overflow pages are done---------------------------------");
			for(int i=0;i<ofIndex.size();i++)
			{
				//	System.out.println(ofIndex.get(i));
				ByteBuffer by=mFile.getBuffer(ofIndex.get(i));
				DatumBuffer searchDatum=new DatumBuffer(by, keySpec.rowSchema());
				for(int tuples=0;tuples<searchDatum.length();tuples++)
				{
					Datum[] tempresult=searchDatum.read(tuples);
					if(Datum.compareRows(keySpec.createKey(tempresult),key)==0)
					{
						//	System.out.println("keys are equal");
						//	System.out.println(tempresult);
						
						results.add(tempresult);
					}
				}
			}
			
			/*for(int i=0;i<results.size();i++)
			{
				Datum[] d=(results.get(i));
				for(int r=0;r<d.length;r++)
				{
					System.out.print(d[r].toString()+"   ----   ");	
				}
				System.out.println();

			}*/
			 
			return results;


		}

	}

	@Override
	public IndexIterator scan() throws SqlException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IndexIterator rangeScanTo(Datum[] toKey) throws SqlException,
	IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IndexIterator rangeScanFrom(Datum[] fromKey) throws SqlException,
	IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IndexIterator rangeScan(Datum[] start, Datum[] end)
			throws SqlException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Datum[] get(Datum[] key) throws SqlException, IOException {
		// TODO Auto-generated method stub
		return null;
	}


}