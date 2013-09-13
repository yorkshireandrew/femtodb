package femtodb;

import femtodbexceptions.FemtoDBIOException;

public interface FemtoDBIterator {
	boolean 		hasNext();				
	RowAccessType 	next() 		throws FemtoDBIOException;
	void 			remove() 	throws UnsupportedOperationException;
	void			reset();	
}
