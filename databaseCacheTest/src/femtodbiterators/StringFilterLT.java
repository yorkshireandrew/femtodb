package femtodbiterators;

import femtodb.FemtoDBIterator;
import femtodb.RowAccessType;
import femtodbexceptions.FemtoDBConcurrentModificationException;
import femtodbexceptions.FemtoDBIOException;

public class StringFilterLT implements FemtoDBIterator{
	private int 			column;
	private String 			compareValue;
	private FemtoDBIterator source;
	private boolean			invert;
	private RowAccessType	currentRow;
	
	StringFilterLT(final int column, final String compareValue, final FemtoDBIterator source, final boolean invert)
	{
		this.column 		= column;
		this.compareValue 	= compareValue;
		this.source			= source;
		this.invert 		= invert;
	}
	
	@Override
	public boolean hasNext() throws FemtoDBConcurrentModificationException,FemtoDBIOException {
		int columnL 			= column;
		String compareValueL 	= compareValue;
		FemtoDBIterator sourceL = source;
		boolean invertL			= invert;
		while(true)
		{
			if(sourceL.hasNext() == false)return false;
			RowAccessType	temp = sourceL.next();
			if(temp.getString(columnL).compareTo(compareValueL) < 0)
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
