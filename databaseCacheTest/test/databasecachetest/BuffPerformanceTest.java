package databasecachetest;

public class BuffPerformanceTest {
	
	public static void main(String[] args) {
		BuffPerformanceTest ut = new BuffPerformanceTest();
		ut.run();
	}
	
	public void run()
	{
		
		long t1,t2,t3,t4,t5,t6,t7,t8,t9,t10;
		int INNER = 1000;
		int SIZE = 100000;
		byte[] buffer = new byte[8];
		
		Long[] longArray = new Long[SIZE];
		
		for(int y = 0; y < SIZE; y++)
		{
			longArray[y] = new Long(y);
		}
		
		byte[] data = new byte[(SIZE << 3)];
		long l=0;
		for(int outer = 0; outer < 4; outer++)
		{
			// fill Long array
			t1 = System.currentTimeMillis();
			for(int x = 0; x < INNER; x++)
			{
				for(int y = 0; y < SIZE; y++)
				{
					BuffWrite.writeLong2(data,(y << 3),y);
				}
			}
			t2 = System.currentTimeMillis();
			
			
			// fill Long array2
			t3 = System.currentTimeMillis();
			for(int x = 0; x < INNER; x++)
			{
				for(int y = 0; y < SIZE; y++)
				{
					BuffWrite.writeLong3(data,(y << 3),y,buffer);
				}
			}
			t4 = System.currentTimeMillis();
			
			// read data
			t5 = System.currentTimeMillis();
			for(int x = 0; x < INNER; x++)
			{
				for(int y = 0; y < SIZE; y++)
				{
					l = BuffRead.readLong2(data, (y << 3));
				}
			}
			t6 = System.currentTimeMillis();
			
			// read data2
			t7 = System.currentTimeMillis();
			for(int x = 0; x < INNER; x++)
			{
				for(int y = 0; y < SIZE; y++)
				{
					l = BuffRead.readLong3(data, (y << 3),buffer);
				}
			}
			t8 = System.currentTimeMillis();
			
			// read long array
			t9 = System.currentTimeMillis();
			for(int x = 0; x < INNER; x++)
			{
				for(int y = 0; y < SIZE; y++)
				{
					Long longy = longArray[y];
					if(longy != null)l = longy;
				}
			}
			t10 = System.currentTimeMillis();
			
			long w1 = t2-t1;
			long w2 = t4-t3;
			long r1 = t6-t5;
			long r2 = t8-t7;
			long x1 = t10-t9;
			System.out.println("------------");
			System.out.println("w1 " + w1);
			System.out.println("w2 " + w2);
			System.out.println("r1 " + r1);
			System.out.println("r2 " + r2);
			System.out.println("x1 " + x1);
		}
		System.out.println("l " + l);	
	}
	
	/*
	 * ------------
w1 7719
w2 9578
r1 8235
r2 11437
x1 500
------------
w1 7078
w2 9609
r1 8735
r2 11297
x1 468
------------
w1 7485
w2 10531
r1 8203
r2 12281
x1 485
------------
w1 7578
w2 9547
r1 8250
r2 11234
x1 484
l 99999
*/
	
}
