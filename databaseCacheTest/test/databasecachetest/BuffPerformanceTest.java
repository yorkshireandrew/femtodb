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
					BuffWrite.writeLong(data,(y << 3),y);
				}
			}
			t2 = System.currentTimeMillis();
			
			
			// fill Long array2
			t3 = System.currentTimeMillis();
			for(int x = 0; x < INNER; x++)
			{
				for(int y = 0; y < SIZE; y++)
				{
					BuffWrite.writeLong2(data,(y << 3),y);
				}
			}
			t4 = System.currentTimeMillis();
			
			// read data
			t5 = System.currentTimeMillis();
			for(int x = 0; x < INNER; x++)
			{
				for(int y = 0; y < SIZE; y++)
				{
					l = BuffRead.readLong(data, (y << 3));
				}
			}
			t6 = System.currentTimeMillis();
			
			// read data2
			t7 = System.currentTimeMillis();
			for(int x = 0; x < INNER; x++)
			{
				for(int y = 0; y < SIZE; y++)
				{
					l = BuffRead.readLong2(data, (y << 3));
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
	------------
	w1 11484
	w2 7047
	r1 9828
	r2 8891
	x1 500
	------------
	w1 11187
	w2 7063
	r1 9797
	r2 8047
	x1 468
	------------
	w1 13657
	w2 8875
	r1 9890
	r2 8063
	x1 468
	------------
	w1 11204
	w2 7046
	r1 9735
	r2 8140
	x1 469
	l 99999
	*/
	
}
