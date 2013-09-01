package femtodb;

public class LongObjectOverheadTest {

	static final int ARRAYSIZE = 100000;
	static final int INNERLOOP = 1000;
	
	public static void main(String[] args) {
		LongObjectOverheadTest x = new LongObjectOverheadTest();
		x.execute();
	}
	
	public final void execute()
	{
		Long[] longObjectArray = new Long[ARRAYSIZE];
		long[] longArray = new long[ARRAYSIZE];
		
		long t1,t2,t3,t4,t5,t6,t7,t8;
		
		for(int outer = 0; outer < 4; outer++)
		{
			
			// filling object array
			t1 = System.currentTimeMillis();
			for(int inner = 0; inner < INNERLOOP; inner++)
			{
				for(int x = 0; x < ARRAYSIZE; x++)
				{
					longObjectArray[x] = new Long(x);
				}				
			}
			t2 = System.currentTimeMillis();
			
			// filling array
			t3 = System.currentTimeMillis();
			for(int inner = 0; inner < INNERLOOP; inner++)
			{
				for(int x = 0; x < ARRAYSIZE; x++)
				{
					longArray[x] = x;
				}				
			}
			t4 = System.currentTimeMillis();
			
			long temp = 0;
			// reading  object array
			t5 = System.currentTimeMillis();
			for(int inner = 0; inner < INNERLOOP; inner++)
			{
				for(int x = 0; x < ARRAYSIZE; x++)
				{
					temp += longObjectArray[x];
				}				
			}
			t6 = System.currentTimeMillis();	
			
			// reading  array
			t7 = System.currentTimeMillis();
			long special = Long.MAX_VALUE;
			for(int inner = 0; inner < INNERLOOP; inner++)
			{
				for(int x = 0; x < ARRAYSIZE; x++)
				{
					long l = longArray[x];
					if(l == special)l = 0;
					temp += l;
				}				
			}
			t8 = System.currentTimeMillis();
			
			long w1 = t2 - t1;
			long w2 = t4 - t3;
			long r1 = t6 - t5;
			long r2 = t8 - t7;
			
			System.out.println(temp);
			System.out.println("obj w = " + w1);
			System.out.println("arr w = " + w2);
			System.out.println("obj r = " + r1);
			System.out.println("arr r = " + r2);
			System.out.println("--------------");
		}
	}
	
	/* results
	 * 9999900000000
obj w = 8468
arr w = 360
obj r = 719
arr r = 843
--------------
9999900000000
obj w = 8063
arr w = 344
obj r = 796
arr r = 875
--------------
9999900000000
obj w = 8110
arr w = 344
obj r = 593
arr r = 813
--------------
9999900000000
obj w = 8110
arr w = 344
obj r = 625
arr r = 843
--------------
*/

}
