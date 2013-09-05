package femtodb;

public class WriteReadTest {

	public static void main(String[] args) {

		byte[] test = new byte[8];
		int tes = -1;
		int tes2 = tes >>> 24;
		System.out.println("tes2 =" +tes2);
		tes2 = tes2 & 0xFF;
		tes = tes2 << 24;
		System.out.println("tes=" + tes);
		tes = tes | 0x00FFFFFF;
		System.out.println("tes=" + tes);
		System.out.println("-----------");
		int g = 255;
		byte b = (byte)g;
		int bb = b;
		System.out.println("bb=" + bb);
		
		for(long x = 0; x < 70000; x++)
		{
			BuffWrite.writeLong(test,0,x);
			long y = BuffRead.readLong(test,0);
			if( x != y){System.out.println("" + x + " ==> " + y);break;}
		}
		
		long x = -1;
		BuffWrite.writeLong(test,0,x);
		long y = BuffRead.readLong(test,0);
		if( x != y){System.out.println("" + x + " ==> " + y);}

		x = -2;
		BuffWrite.writeLong(test,0,x);
		y = BuffRead.readLong(test,0);
		if( x != y){System.out.println("" + x + " ==> " + y);}
		
		x = Long.MAX_VALUE;
		BuffWrite.writeLong(test,0,x);
		y = BuffRead.readLong(test,0);
		if( x != y){System.out.println("" + x + " ==> " + y);}
		
		x = Long.MIN_VALUE;
		BuffWrite.writeLong(test,0,x);
		y = BuffRead.readLong(test,0);
		if( x != y){System.out.println("" + x + " ==> " + y);}

	}

}
