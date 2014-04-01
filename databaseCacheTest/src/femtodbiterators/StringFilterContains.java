package femtodbiterators;

import femtodb.FemtoDBIterator;
import femtodb.RowAccessType;
import femtodbexceptions.FemtoDBConcurrentModificationException;
import femtodbexceptions.FemtoDBIOException;

public class StringFilterContains implements FemtoDBIterator{
	private int 			column;
	private String 			compareValue;
	private FemtoDBIterator source;
	private RowAccessType	currentRow;
	
	StringFilterContains(final int column, final String compareValue, final FemtoDBIterator source)
	{
		this.column 		= column;
		this.compareValue 	= compareValue;
		this.source			= source;
	}
	
	@Override
	public boolean hasNext() throws FemtoDBConcurrentModificationException,FemtoDBIOException {
		int columnL 			= column;
		String compareValueL 	= compareValue;
		FemtoDBIterator sourceL = source;
		while(true)
		{
			if(sourceL.hasNext() == false)return false;
			RowAccessType	temp = sourceL.next();
			if(temp.getString(columnL).contains(compareValueL))
			{
				currentRow = temp;
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
