package femtodb;

/** Interface used for iterating over the rows of a tableCore */
import femtodbexceptions.FemtoDBConcurrentModificationException;
import femtodbexceptions.FemtoDBIOException;

public interface FemtoDBIterator {
	boolean 		hasNext() throws FemtoDBConcurrentModificationException,FemtoDBIOException;				
	RowAccessType 	next() 	throws FemtoDBConcurrentModificationException,FemtoDBIOException;
	void 			remove() 	throws UnsupportedOperationException, FemtoDBConcurrentModificationException, FemtoDBIOException;
	void			reset();	
	void			 setToo(final long startPoint) throws FemtoDBIOException; 
}
