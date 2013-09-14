package femtodb;

import femtodbexceptions.FemtoDBConcurrentModificationException;
import femtodbexceptions.FemtoDBIOException;

public interface FemtoDBIterator {
	boolean 		hasNext() throws FemtoDBConcurrentModificationException,FemtoDBIOException;				
	RowAccessType 	next(long serviceNumber) 	throws FemtoDBConcurrentModificationException,FemtoDBIOException;
	void 			remove(long serviceNumber) 	throws UnsupportedOperationException, FemtoDBConcurrentModificationException, FemtoDBIOException;
	void			reset();	
}
