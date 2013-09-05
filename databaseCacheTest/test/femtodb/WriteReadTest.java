package femtodb;

public class WriteReadTest {

	public static void main(String[] args) {

		byte[] test = new byte[4];
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
		
		for(int x = 0; x < 70000; x++)
		{
			BuffWrite.writeInt(test,0,x);
			int y = BuffRead.readInt(test,0);
			if( x != y){System.out.println("" + x + " ==> " + y);break;}
		}
		

	}

}
