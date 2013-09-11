package femtodb;

import java.io.IOException;

public interface FemtoDBIterator {
	boolean 		hasNext();				
	RowAccessType 	next() 		throws IOException;
	void 			remove() 	throws UnsupportedOperationException;
	void			reset();	
}
