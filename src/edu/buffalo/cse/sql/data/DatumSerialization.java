/**
 * Utility methods for serializing Datum values.  Specifically handy when the
 * data being read from/written to is of variable size.
 * 
 * getLength() - Returns the number of bytes required to store the Datum/row
 * 
 * readLen() - Returns the number of bytes that the row stored in the ByteBuffer
 *             at the specified position (with the specified schema) takes up.
 * 
 * write() - Write the specified Datum/row to the ByteBuffer at the specified 
 *           position.  Returns the number of bytes read/written
 * 
 * read() - Read the Datum/row with the specified schema from the ByteBuffer
 *          at the specified position.  Returns the Datum/row read.
 **/

package edu.buffalo.cse.sql.data;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.data.Datum;

public class DatumSerialization {

	public static int getLength(Datum d)
	{
		switch(d.getType()){
		case INT: return 4;
		case FLOAT: return 4;
		case STRING: return 4+(((Datum.Str)d).s.getBytes().length);
		//case STRING : return 4+new String(d.toString()).getBytes().length;
		case BOOL: return 1;
		}
		return 100000;
	}

	public static int getLength(Datum[] row){
		int len = 0;
		for(Datum d : row){ len += getLength(d); }
		return len;
	}

	public static int readLen(ByteBuffer buffer, int position, Schema.Type t)
	{
		switch(t){
		case INT: return 4;
		case FLOAT: return 4;
		case BOOL: return 4;
		case STRING: return buffer.getInt(position) + 4;
		}
		return 100000;
	}

	public static int write(ByteBuffer buffer, int position, Datum d)
	{
		try {
			switch(d.getType()){
			case INT: buffer.putInt(position, d.toInt()); return 4;
			case FLOAT: buffer.putFloat(position, ((Datum.Flt)d).f); return 4;
			case BOOL: buffer.put(position, (byte)(((Datum.Bool)d).b ? 1 : 0)); 
			return 1;
			case STRING: {
				//String s=new String(d.toString());
				 String s = ((Datum.Str)d).s;
				byte[] sbytes = s.getBytes();
				buffer.putInt(position, sbytes.length);
				int i = 4;
				for(byte b : sbytes){
					buffer.put(position + i, b);
					i++;
				}
		//		System.out.println("writing into string"+buffer.toString());
				
				return i; 
			}
			}
			//this should never happen;
		} catch (Datum.CastError e){}
		return 100000; 
	}

	public static int write(ByteBuffer buffer, int position, Datum[] row)
	{ 
		int i = 0;
		for(Datum d : row){
			i += write(buffer, position+i, d);
		}
		return i;
	}

	public static Datum read(ByteBuffer buffer, int position, Schema.Type t) throws UnsupportedEncodingException
	{
		switch(t){
		case INT: return new Datum.Int(buffer.getInt(position));
		case FLOAT: return new Datum.Flt(buffer.getFloat(position));
		case BOOL: return new Datum.Bool(buffer.get(position) > 0);
		case STRING: {
			byte[] sbytes = new byte[buffer.getInt(position)];

			for(int i = 0; i < sbytes.length; i++){
				sbytes[i] = buffer.get(position+4+i);
			}
			return new Datum.Str(new String(sbytes,"UTF-8"));
		}
		}
		return null;
	}

	public static Datum[] read(ByteBuffer buffer, int position, Schema.Type[] t) throws UnsupportedEncodingException
	{
		Datum[] row = new Datum[t.length];
		int offset = 0;
		for(int i = 0; i < t.length; i++){
			row[i] = read(buffer, position + offset, t[i]);
			offset += readLen(buffer, position + offset, t[i]);
		}
		return row;
	}
}