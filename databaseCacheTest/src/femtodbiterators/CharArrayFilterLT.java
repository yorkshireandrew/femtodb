package femtodbiterators;

import java.util.Arrays;

import femtodb.FemtoDBIterator;
import femtodb.RowAccessType;
import femtodbexceptions.FemtoDBConcurrentModificationException;
import femtodbexceptions.FemtoDBIOException;

public class CharArrayFilterLT implements FemtoDBIterator{
	private int 			column;
	private char[] 			compareValue;
	private FemtoDBIterator source;
	private boolean			invert;
	private RowAccessType	currentRow;
	
	CharArrayFilterLT(final int column, final char[] compareValue, final FemtoDBIterator source, final boolean invert)
	{
		this.column 		= column;
		this.compareValue 	= compareValue;
		this.source			= source;
		this.invert 		= invert;
	}
	
	@Override
	public boolean hasNext() throws FemtoDBConcurrentModificationException,FemtoDBIOException {
		int columnL 				= column;
		char[] compareValueL 		= compareValue;
		FemtoDBIterator sourceL 	= source;
		boolean invertL				= invert;
		while(true)
		{
			if(sourceL.hasNext() == false)return false;
			RowAccessType	temp = sourceL.next();
			if(isLessThan(temp.get_charArray(columnL),compareValueL))
			{
				if(!invertL){currentRow = temp;return true;}
			}
			else
			{
				if(invertL){currentRow = temp;return true;}
			}
		}
	}

	@Override
	public RowAccessType next() throws FemtoDBConcurrentModificationException,FemtoDBIOException {
			return currentRow;
	}

	@Override
	public void remove() throws UnsupportedOperationException,FemtoDBConcurrentModificationException, FemtoDBIOException {
			source.remove();
	}

	@Override
	public void reset() {
			source.reset();
	}
	
	final private boolean isLessThan(char[] a, char[] b)
	{
		int lena = a.length;
		int lenb = b.length;
		int minlen = lena;
		if(lenb < minlen)minlen = lenb;
		for(int x = 0; x < minlen; x++)
		{
			if(a[x] < b[x]) return true;
		}
		return false;	
	}
	
	@Override
	public void setToo(long startPoint) throws FemtoDBIOException {
		source.setToo(startPoint);	
	}

}
