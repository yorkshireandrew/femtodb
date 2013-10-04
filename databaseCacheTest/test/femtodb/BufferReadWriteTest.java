package femtodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import femtodbexceptions.FemtoBDCharArrayExceedsColumnSizeException;
import femtodbexceptions.FemtoDBStringExceedsColumnSizeException;
import java.io.UTFDataFormatException;

public class BufferReadWriteTest {
	byte[] buff;
	@Before
	public void setUp() throws Exception {
		buff = new byte[20];
	}
	
	
	@After
	public void tearDown() throws Exception {	
	}
	
	
	//********************** testWriteReadDelete ***********************
	
	@Test
	public void testByte()
	{
		BuffWrite.write(buff, 1, 10);
		BuffWrite.write(buff, 2, 20);
		assertEquals(10, BuffRead.read(buff, 1));
		assertEquals(20, BuffRead.read(buff, 2));
	}
	
	@Test
	public void testBoolean()
	{
		BuffWrite.writeBoolean(buff, 1, true);
		BuffWrite.writeBoolean(buff, 2, false);
		assertTrue(BuffRead.readBoolean(buff, 1));
		assertFalse(BuffRead.readBoolean(buff, 2));
	}
	
	@Test
	public void testWriteBytes1()
	{
		byte[] toWrite = new byte[2];
		toWrite[0] = 10;
		toWrite[1] = 20;
		BuffWrite.writeByteArray(buff, 1, toWrite);
		
		byte[] readBack = BuffRead.readByteArray(buff, 1);
		assertEquals(2, readBack.length);
		assertEquals(10, readBack[0]);
		assertEquals(20, readBack[1]);	
	}
	
	@Test
	public void testWriteBytes2()
	{
		byte[] toWrite = new byte[7];
		toWrite[0] = 00;
		toWrite[1] = 00;
		
		// This writeBytes method is simple low-level arrayCopy
		// build something the same as writing byte[] = { 77 }
		
		toWrite[2] = 00;
		toWrite[3] = 00;
		toWrite[4] = 00;
		toWrite[5] = 01;
		
		toWrite[6] = 77;
		
		BuffWrite.writeBytes(buff, 2, toWrite, 2,5);
		
		byte[] readBack = BuffRead.readByteArray(buff, 2);
		assertEquals(1, readBack.length);
		assertEquals(77, readBack[0]);
	}
	
	@Test
	public void testWriteChar()
	{
		char c = 'x';
		BuffWrite.writeChar(buff, 1, c);
		
		char readBack = BuffRead.readChar(buff, 1);
		assertEquals(c, readBack);
	}
	
	@Test
	public void testWriteChars()
	{
		char[] chars = {'f','o','o','b','a','r'};
		
		try {
			BuffWrite.writeCharArray(buff, 1, chars, 14);
		} catch (FemtoBDCharArrayExceedsColumnSizeException e) {
			fail();
		}
		
		char[] readBack = BuffRead.readCharArray(buff, 1);
		assertEquals(6, readBack.length);
		assertEquals('f',readBack[0]);
		assertEquals('o',readBack[1]);
		assertEquals('o',readBack[2]);
		assertEquals('b',readBack[3]);
		assertEquals('a',readBack[4]);
		assertEquals('r',readBack[5]);
		
		// check exception thrown if limit it too low
		try {
			BuffWrite.writeCharArray(buff, 1, chars, 12);
			fail();
		} catch (FemtoBDCharArrayExceedsColumnSizeException e) {}
	}
	
	@Test
	public void testWriteDouble()
	{
		double d = 123.456;
		BuffWrite.writeDouble(buff, 1, d);
		double readBack = BuffRead.readDouble(buff,1);
		double diff = d - readBack;
		if(diff != 0)fail();
		
		d = -d;
		BuffWrite.writeDouble(buff, 1, d);
		readBack = BuffRead.readDouble(buff,1);
		diff = d - readBack;
		if(diff != 0)fail();
	}
	
	@Test
	public void testWriteFloat()
	{
		float f = 123.456F;
		BuffWrite.writeFloat(buff, 1, f);
		float readBack = BuffRead.readFloat(buff,1);
		float diff = f - readBack;
		if(diff != 0)fail();
		
		f = -f;
		BuffWrite.writeFloat(buff, 1, f);
		readBack = BuffRead.readFloat(buff,1);
		diff = f - readBack;
		if(diff != 0)fail();
	}
	
	
	@Test
	public void testWriteInt()
	{
		int f = 123456;
		BuffWrite.writeInt(buff, 1, f);
		int readBack = BuffRead.readInt(buff,1);
		int diff = f - readBack;
		if(diff != 0)fail();
		
		f = -f;
		BuffWrite.writeInt(buff, 1, f);
		readBack = BuffRead.readInt(buff,1);
		diff = f - readBack;
		if(diff != 0)fail();		
	}
	
	@Test
	public void testWriteLong()
	{
		long f = 123456L;
		BuffWrite.writeLong(buff, 1, f);
		long readBack = BuffRead.readLong(buff,1);
		long diff = f - readBack;
		if(diff != 0)fail();
		
		f = -f;
		BuffWrite.writeLong(buff, 1, f);
		readBack = BuffRead.readLong(buff,1);
		diff = f - readBack;
		if(diff != 0)fail();		
	}
	
	
	@Test
	public void testWriteString1()
	{
		String test = "foobar";
		try {
			BuffWrite.writeString(buff,1,19,test);
		} catch (FemtoDBStringExceedsColumnSizeException e) {
			fail();
		}
		
		String readBack = null;
		try {
			readBack = BuffRead.readString(buff,1);
		} catch (UTFDataFormatException e) {
			fail();
		}

		assertEquals(test.length(),readBack.length());
		assertEquals(test,readBack);
		
		// check exception thrown if insufficient space
		try {
			BuffWrite.writeString(buff,1,7,test);
			fail();
		} catch (FemtoDBStringExceedsColumnSizeException e) {}
	}
	
	
	/** test using stringBuffer */
	@Test
	public void testWriteString2()
	{
		String test = "foobar";
		try {
			BuffWrite.writeString(buff,1,19,test);
		} catch (FemtoDBStringExceedsColumnSizeException e) {
			fail();
		}
		
		StringBuilder readBack = new StringBuilder();
		readBack.append("tango wango fango bango gango");
		try {
			readBack = BuffRead.readStringBuilder(buff,1,readBack);
		} catch (UTFDataFormatException e) {
			fail();
		}

		String readBackString = readBack.toString();
		assertEquals(readBackString,test);
	}
	
	
	
	
	
	
}
