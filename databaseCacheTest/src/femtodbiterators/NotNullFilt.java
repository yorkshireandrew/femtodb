package femtodbiterators;

import femtodb.FemtoDBIterator;
import femtodb.RowAccessType;
import femtodbexceptions.FemtoDBConcurrentModificationException;
import femtodbexceptions.FemtoDBIOException;

public class NotNullFilt implements FemtoDBIterator{
	
	private FemtoDBIterator source;
	private int 			column;
	RowAccessType 			nextValue;
	NotNullFilt(FemtoDBIterator source, int column)
	{
		this.source = source;
		this.column = column;
	}
	
	@Override
	public boolean hasNext() throws FemtoDBConcurrentModificationException,FemtoDBIOException {
		boolean found = false;
		FemtoDBIterator sourceL = source;
		int columnL = column;
		while(!found)
		{
			if(sourceL.hasNext() == false)return false;
			RowAccessType rat = sourceL.next();
			if(rat.isColumnNull(columnL)){nextValue = rat; return true;}		
		}
		return false;
	}

	@Override
	public RowAccessType next() throws FemtoDBConcurrentModificationException,FemtoDBIOException {
		return nextValue;
	}

	@Override
	public void remove() throws UnsupportedOperationException,FemtoDBConcurrentModificationException, FemtoDBIOException {
		if(nextValue != null)source.remove();
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
