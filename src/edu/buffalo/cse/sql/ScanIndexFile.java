package edu.buffalo.cse.sql;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.index.IndexKeySpec;
import edu.buffalo.cse.sql.plan.ScanNode;

public class ScanIndexFile {

	public static ArrayList<Datum[]> ScanIndex(Datum[] key,IndexKeySpec keySpec,FileManager fm, File path,int dirSize) throws SqlException, IOException
	{

		//BufferedReader br=new BufferedReader(new FileReader("dirInfo.txt"));
		//int dirSize=Integer.parseInt(br.readLine().trim());
		HashMap<Integer, ArrayList<Integer>> flowStruct=new HashMap<Integer, ArrayList<Integer>>();
	//	System.out.println("directory size is "+dirSize);
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
				int k=0;
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
			System.out.println("--------------------overflow pages are done---------------------------------");
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
			for(int i=0;i<results.size();i++)
			{
				Datum[] d=(results.get(i));
				for(int r=0;r<d.length;r++)
				{
					System.out.print("here "+new String(d[r].toString())+"---- ");	
				}
				System.out.println();

			}

			return results;


		}

	}
	

}
