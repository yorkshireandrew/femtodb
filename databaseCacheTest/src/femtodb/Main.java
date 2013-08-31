package femtodb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;

// Int		4
// Long 	8
// Float	4
// Double	8
// Char		2
// Chars	2 * number of characters in string (excluding null term)

// speed with UTF encoding 1000000 loops of "1234567890"
// write = 672
// read = 359

// speed using char array adding null terminator
// without correct null terminator detection
//write = 828
//read = 766

// speed using char array - using string builder to read back
// write = 844
// read = 1125

public class Main {

	public static void main(String[] args) {
		Main ut = new Main();
		ut.run();
	}
	
	public void run()
	{
		final int LOOPS = 1000000;
		ByteOutputStream bos = new ByteOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		long t1,t2,t3,t4;
		t1 = 0;
		t2 = 0;
		t3 = 0;
		t4 = 0;
		try {
//			dos.writeChar('x');
			t1 = System.currentTimeMillis();
			for(int x = 0; x < LOOPS; x++){
				dos.writeChars("1234567890");
				dos.writeChar('\0');
			}
			t2 = System.currentTimeMillis();
			dos.close();
		} catch (IOException e) {}
		
		int len = bos.size();
		System.out.println("size=" + len);
		
		ByteArrayInputStream bais = new ByteArrayInputStream(bos.getBytes());
		DataInputStream dis = new DataInputStream(bais);
		String outstring = null;
		StringBuffer sb = new StringBuffer();
		try {
			t3 = System.currentTimeMillis();
			for(int x = 0; x < LOOPS; x++){
				boolean loopy = true;
				sb.setLength(0);
				while(loopy)
				{
					char c = dis.readChar();
					if( c != '\0')
					{
						sb.append(c);
					}
					else
					{
						loopy = false;
					}
				}
				outstring = sb.toString();
			}
			t4 = System.currentTimeMillis();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(outstring);
		System.out.println("write = " + (t2-t1));
		System.out.println("read = " + (t4-t3));
		
	}

}
