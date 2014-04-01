package femtodbiterators;

import femtodb.FemtoDBIterator;
import femtodb.RowAccessType;
import femtodbexceptions.FemtoDBConcurrentModificationException;
import femtodbexceptions.FemtoDBIOException;

public class CharFilterLT implements FemtoDBIterator{
	private int 			column;
	private char 			compareValue;
	private FemtoDBIterator source;
	private boolean			invert;
	private RowAccessType	currentRow;
	
	CharFilterLT(final int column, final char compareValue, final FemtoDBIterator source, final boolean invert)
	{
		this.column 		= column;
		this.compareValue 	= compareValue;
		this.source			= source;
		this.invert 		= invert;
	}
	
	@Override
	public boolean hasNext() throws FemtoDBConcurrentModificationException,FemtoDBIOException {
		int columnL 			= column;
		char compareValueL 		= compareValue;
		FemtoDBIterator sourceL = source;
		boolean invertL			= invert;
		while(true)
		{
			if(sourceL.hasNext() == false)return false;
			RowAccessType	temp = sourceL.next();
			if(temp.get_char(columnL) < compareValueL)
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
	
	@Override
	public void setToo(long startPoint) throws FemtoDBIOException {
		source.setToo(startPoint);	
	}
	

}
