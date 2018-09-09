# femtodb
Tiny DB key-value store that caches and saves aligned tables. Using ping-pong copying providing resiliance.

Inspired by SQLLite I wrote this. 
Unlike SQLLite that stores things in a single file this stores its information in two seperate directories.
These contain a file indicating if the save when the program is closed completed, if not the other directory is used.

The idea is you periodically call backup() which causes any unsaved data in the caches to be flushed.
That way the delay femtoDB adds during shutdown is small.

If the server goes down halfway through shutting down or backup call the ping or pong directory is marked incomplete
so the other is used.

Each table is stored as a cluster of files. As rows are deleted the file cluster gets re-arranged to release storage.

With each table the in memory cache associated with it can be set by calling its setCacheSize method.

With each table the number of rows each file can also be set using the setRowsPerFile method.
So you can tune this to improve performance.

Because the data in each table is aligned I was writing iterators so you can perform queries against
columns other than the primary key. This is not fully complete.






