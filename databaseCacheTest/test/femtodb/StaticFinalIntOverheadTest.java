package femtodb;

public class StaticFinalIntOverheadTest {

	private static final int ONE 	= 1;
	private static final int TWO 	= 2;
	private static final int THREE 	= 3;
	private static final int FOUR 	= 4;
	private static final int FIVE 	= 5;
	private static final int SIX 	= 6;
	private static final int SEVEN 	= 7;
	private static final int EIGHT 	= 8;
	private static final int NINE 	= 9;
	private static final int TEN 	= 10;
	private static final int INNERLOOP = 10000000;

	
	public static void main(String[] args) {
		StaticFinalIntOverheadTest x = new StaticFinalIntOverheadTest();
		x.execute();
	}
	
	public final void execute()
	{		
		long t1,t2,t3,t4,t5,t6;
		
		for(int outer = 0; outer < 4; outer++)
		{
			// create local variables
			int one 	= 1;
			int two 	= 2;
			int three	= 3;
			int four	= 4;
			int five	= 5;
			int six		= 6;
			int seven	= 7;
			int eight	= 8;
			int nine	= 9;
			int ten		= 10;
			
			// using static final int
			int temp = 0;
			t1 = System.currentTimeMillis();
			for(int inner = 0; inner < INNERLOOP; inner++)
			{
				temp += ONE;			
				temp += TWO;			
				temp += THREE;			
				temp += FOUR;			
				temp += FIVE;			
				temp += SIX;			
				temp += SEVEN;			
				temp += EIGHT;			
				temp += NINE;			
				temp += TEN;			
			}
			t2 = System.currentTimeMillis();
			
			// using local variables for local people
			t3 = System.currentTimeMillis();
			for(int inner = 0; inner < INNERLOOP; inner++)
			{
				temp += one;			
				temp += two;			
				temp += three;			
				temp += four;			
				temp += five;			
				temp += six;			
				temp += seven;			
				temp += eight;			
				temp += nine;			
				temp += ten;			
			}
			t4 = System.currentTimeMillis();
			
			t5 = System.currentTimeMillis();
			for(int inner = 0; inner < INNERLOOP; inner++)
			{
				temp += SomeOtherClass.ONE;			
				temp += SomeOtherClass.TWO;			
				temp += SomeOtherClass.THREE;			
				temp += SomeOtherClass.FOUR;			
				temp += SomeOtherClass.FIVE;			
				temp += SomeOtherClass.SIX;			
				temp += SomeOtherClass.SEVEN;			
				temp += SomeOtherClass.EIGHT;			
				temp += SomeOtherClass.NINE;			
				temp += SomeOtherClass.TEN;			
			}
			t6 = System.currentTimeMillis();

			long w1 = t2 - t1;
			long w2 = t4 - t3;
			long w3 = t6 - t5;

			
			System.out.println(temp);
			System.out.println("psfi  = " + w1);
			System.out.println("local = " + w2);
			System.out.println("other = " + w3);
			System.out.println("--------------");
		}
	}
	
	/*
	results
	private static final int INNERLOOP = 100000000;
	-1884901888
psfi  = 734
local = 703
--------------
-1884901888
psfi  = 735
local = 703
--------------
-1884901888
psfi  = 718
local = 704
--------------
-1884901888
psfi  = 750
local = 734
--------------


private static final int INNERLOOP = 10000000;
1100000000
psfi  = 78
local = 63
--------------
1100000000
psfi  = 78
local = 63
--------------
1100000000
psfi  = 78
local = 62
--------------
1100000000
psfi  = 78
local = 63
--------------


private static final int INNERLOOP = 1000000;
110000000
psfi  = 15
local = 0
--------------
110000000
psfi  = 16
local = 0
--------------
110000000
psfi  = 16
local = 0
--------------
110000000
psfi  = 15
local = 0
--------------



private static final int INNERLOOP = 10000000;
1650000000
psfi  = 62
local = 78
other = 79
--------------
1650000000
psfi  = 78
local = 62
other = 78
--------------
1650000000
psfi  = 78
local = 63
other = 78
--------------
1650000000
psfi  = 63
local = 78
other = 62
--------------
*/
	
	
	
}
