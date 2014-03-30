package femtodbiterators;
import femtodb.FemtoDBIterator;
import femtodb.RowAccessType;
import femtodbexceptions.FemtoDBConcurrentModificationException;
import femtodbexceptions.FemtoDBIOException;

/** Factory utility that picks the appropriate FemtoDBIterator filter */
public class Filt{
		
	// enumeration for operator
	private static final int INT_LT						= 0;
	private static final int INT_EQ						= 1;
	private static final int INT_GT						= 2;
	private static final int INT_EQ_NULL				= 3;
	private static final int INT_NOT_NULL				= 4;
	
	// string operators
	private static final int INT_LT_IGNORE_CASE			= 5;
	private static final int INT_EQ_IGNORE_CASE			= 6;
	private static final int INT_GT_IGNORE_CASE			= 7;
	private static final int INT_CONTAINS				= 8;
	private static final int INT_STARTS_WITH			= 9;
	private static final int INT_ENDS_WITH				= 10;
	
	// *******************************************************
	// ****************** CONSTRUCTORS ***********************
	// *******************************************************
	static final FemtoDBIterator getFilt(final int column, final String op, final FemtoDBIterator source)
	{
		switch(calcOp(op)){
		case INT_EQ_NULL:
			return new NullFilt(source, column);
		case INT_NOT_NULL:
			return new NotNullFilt(source, column);
		}
		return null;
	}	
	
	static final FemtoDBIterator getFilt(final int column, final String op, final byte compareValue, final FemtoDBIterator source)
	{
		switch(calcOp(op)){
		case INT_LT:
			return new ByteFilterLT(column, compareValue, source, calcInvert(op));
		case INT_EQ:
			return new ByteFilterEQ(column, compareValue, source, calcInvert(op));
		case INT_GT:
			return new ByteFilterGT(column, compareValue, source, calcInvert(op));
		}
		return null;
	}
	
	static final FemtoDBIterator getFilt(final int column, final String op, final char compareValue, final FemtoDBIterator source)
	{
		switch(calcOp(op)){
		case INT_LT:
			return new CharFilterLT(column, compareValue, source, calcInvert(op));
		case INT_EQ:
			return new CharFilterEQ(column, compareValue, source, calcInvert(op));
		case INT_GT:
			return new CharFilterGT(column, compareValue, source, calcInvert(op));
		}
		return null;
	}	
	
	static final FemtoDBIterator getFilt(final int column, final String op, final char[] compareValue, final FemtoDBIterator source)
	{
		switch(calcOp(op)){
		case INT_LT:
			return new CharArrayFilterLT(column, compareValue, source, calcInvert(op));
		case INT_EQ:
			return new CharArrayFilterEQ(column, compareValue, source, calcInvert(op));
		case INT_GT:
			return new CharArrayFilterGT(column, compareValue, source, calcInvert(op));
		}
		return null;
	}
	
	static final FemtoDBIterator getFilt(final int column, final String op, final double compareValue, final FemtoDBIterator source)
	{
		switch(calcOp(op)){
		case INT_LT:
			return new DoubleFilterLT(column, compareValue, source, calcInvert(op));
		case INT_EQ:
			return new DoubleFilterEQ(column, compareValue, source, calcInvert(op));
		case INT_GT:
			return new DoubleFilterGT(column, compareValue, source, calcInvert(op));
		}
		return null;
	}
	
	static final FemtoDBIterator getFilt(final int column, final String op, final float compareValue, final FemtoDBIterator source)
	{
		switch(calcOp(op)){
		case INT_LT:
			return new FloatFilterLT(column, compareValue, source, calcInvert(op));
		case INT_EQ:
			return new FloatFilterEQ(column, compareValue, source, calcInvert(op));
		case INT_GT:
			return new FloatFilterGT(column, compareValue, source, calcInvert(op));
		}
		return null;
	}
	
	static final FemtoDBIterator getFilt(final int column, final String op, final int compareValue, final FemtoDBIterator source)
	{
		switch(calcOp(op)){
		case INT_LT:
			return new IntFilterLT(column, compareValue, source, calcInvert(op));
		case INT_EQ:
			return new IntFilterEQ(column, compareValue, source, calcInvert(op));
		case INT_GT:
			return new IntFilterGT(column, compareValue, source, calcInvert(op));
		}
		return null;
	}	
	
	static final FemtoDBIterator getFilt(final int column, final String op, final long compareValue, final FemtoDBIterator source)
	{
		switch(calcOp(op)){
		case INT_LT:
			return new LongFilterLT(column, compareValue, source, calcInvert(op));
		case INT_EQ:
			return new LongFilterEQ(column, compareValue, source, calcInvert(op));
		case INT_GT:
			return new LongFilterGT(column, compareValue, source, calcInvert(op));
		}
		return null;
	}	
	
	static final FemtoDBIterator getFilt(final int column, final String op, final short compareValue, final FemtoDBIterator source)
	{
		switch(calcOp(op)){
		case INT_LT:
			return new ShortFilterLT(column, compareValue, source, calcInvert(op));
		case INT_EQ:
			return new ShortFilterEQ(column, compareValue, source, calcInvert(op));
		case INT_GT:
			return new ShortFilterGT(column, compareValue, source, calcInvert(op));
		}
		return null;
	}	
	
	static final FemtoDBIterator getFilt(final int column, final String op, final String compareValue, final FemtoDBIterator source)
	{
		switch(calcOp(op)){
		case INT_LT:
			return new StringFilterLT(column, compareValue, source, calcInvert(op));
		case INT_EQ:
			return new StringFilterEQ(column, compareValue, source, calcInvert(op));
		case INT_GT:
			return new StringFilterGT(column, compareValue, source, calcInvert(op));
		case INT_LT_IGNORE_CASE:
			return new StringFilterLTIgnoreCase(column, compareValue, source);
		case INT_EQ_IGNORE_CASE:
			return new StringFilterEQIgnoreCase(column, compareValue, source);
		case INT_GT_IGNORE_CASE:
			return new StringFilterGTIgnoreCase(column, compareValue, source);
		case INT_CONTAINS:
			return new StringFilterContains(column, compareValue, source);
		case INT_STARTS_WITH:
			return new StringFilterStartsWith(column, compareValue, source);
		case INT_ENDS_WITH:
			return new StringFilterEndsWith(column, compareValue, source);
		}
		return null;
	}
	
	private static final int calcOp(String op)
	{	
		if(op == "<"){return INT_LT;}
		if(op == "=="){return INT_EQ;}
		if(op == "="){return INT_EQ;}
		if(op == ">"){return INT_GT;}
		
		if(op == ">="){return  INT_LT;}
		if(op == "!="){return  INT_EQ;}
		if(op == "<>"){return INT_EQ;}
		if(op == "<="){return INT_GT;}
		
		if(op == "==null"){return INT_EQ_NULL;}
		if(op == "==NULL"){return INT_EQ_NULL;}
		if(op == "=null"){return INT_EQ_NULL;}
		if(op == "=NULL"){return INT_EQ_NULL;}
		
		if(op == "!=null"){return INT_NOT_NULL;}
		if(op == "!=NULL"){return INT_NOT_NULL;}
		if(op == "<>null"){return INT_NOT_NULL;}
		if(op == "<>NULL"){return INT_NOT_NULL;}
		
		if(op == "<IGNORECASE"){return INT_LT_IGNORE_CASE;}
		if(op == "=IGNORECASE"){return INT_EQ_IGNORE_CASE;}
		if(op == "=IGNORECASE"){return INT_EQ_IGNORE_CASE;}
		if(op == ">IGNORECASE"){return INT_GT_IGNORE_CASE;}
		if(op == "CONTAINS"){return INT_CONTAINS;}
		if(op == "CONTAINSIGNORECASE"){return INT_CONTAINS;}
		
		return 0;
	}
	
	private static final boolean calcInvert(String op)
	{
		if(op == "<"){return false;}
		if(op == "=="){return false;}
		if(op == "="){return false;}
		if(op == ">"){return false;}
		
		if(op == ">="){return true;}
		if(op == "!="){return true;}
		if(op == "<>"){return true;}
		if(op == "<="){return true;}
		return false;
	}	
}
