package femtodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import femtodbexceptions.FemtoDBIOException;

/** Implements the databases core functionality, It is responsible for holding the list of tableCore objects as well as providing open, backup and flush the cache functionality. */
public class FemtoDB implements Serializable, Lock{
	private static final long serialVersionUID = 1L;
	private String				name;
	private transient String 	path;
	private List<TableCore> 	tableCores;
	private long 				nextUnusedTableNumber;
	private TableLock			databaseLock;
	
	/** Constructs a database requiring a name as an argument. The setPath method must be also called before the database can be used */
	public FemtoDB(String name)
	{
		this.name				= name;
		path					= null;
		tableCores 				= new ArrayList<TableCore>();
		nextUnusedTableNumber 	= 0L;
		databaseLock			= new TableLock();
	}
	
	/** Creates a new tableCore in the database, requiring a name for the tableCore and (optionally) a name for the primary key column */
	public TableCore createTable(String name, String primaryKeyName)
	{
		long tableNumber 	= nextUnusedTableNumber++;
		TableCore newTable 		= new TableCore(this, name, tableNumber, primaryKeyName);
		tableCores.add(newTable);
		return newTable;
	}
	
	/** Creates a backup the database to the path given as an argument. The method automatically creates ping and pong subdirectories if they do not exist. If a backup exists the method will overwrite the oldest (or the most invalid) ping or pong backup. */
	public void backup(String path) throws FileNotFoundException, FemtoDBIOException
	{
		// ensure the path exists
		File backupLocation = new File(path);
		if(!backupLocation.exists()) throw new FileNotFoundException("The following backup location does not exist: " + path);
		
		// ensure there is a ping directory
		String pingDirectoryString = path + File.separator + "ping";
		File ping = new File(pingDirectoryString);
		if(!ping.exists())
		{
			if(!ping.mkdir()) throw new FemtoDBIOException("Database " + name + " was unable to create the following directory when backing up: " + pingDirectoryString);
		}
		
		// ensure there is a pong directory
		String pongDirectoryString = path + File.separator + "pong";
		File pong = new File(pongDirectoryString);
		if(!pong.exists())
		{
			if(!pong.mkdir()) throw new FemtoDBIOException("Database " + name + " was unable to create the following directory when backing up: " + pongDirectoryString);
		}
		

		// Decide if to write to ping or pong
		long 	validPingDatabaseStart = getDatabaseStart(pingDirectoryString, false);	
		long 	validPongDatabaseStart = getDatabaseStart(pongDirectoryString, false);
		boolean usePong = false;	
		if(validPingDatabaseStart == -1L)
		{
			// ping is invalid so use it for backing up
			usePong = false;
		}
		else
		{
			if(validPongDatabaseStart == -1)
			{
				// ping is valid but pong is not so use it for backing up
				usePong = true;
			}
			else
			{
				if(validPingDatabaseStart > validPongDatabaseStart)
				{
					// ping is younger than pong, so use pong for backing up
					usePong = true;
				}
				else
				{
					usePong = false;
				}				
			}
		}
		
		if(usePong)
		{
			if(validPongDatabaseStart == -1)
			{
				backupCompletelyTo(pongDirectoryString);
			}
			else
			{
				backupIncrementalTo(pongDirectoryString);
			}
		}
		else
		{
			if(validPingDatabaseStart == -1)
			{
				backupCompletelyTo(pingDirectoryString);
			}
			else
			{
				backupIncrementalTo(pingDirectoryString);
			}			
		}
	}
	
	private void backupCompletelyTo(String destString) throws FileNotFoundException, FemtoDBIOException
	{
		if(path == null)throw new FemtoDBIOException("Database " + name + " backup was attempted before the database path was set");
		// remove the old directory if it exists and generate a new one
		File directoryFile = new File(destString);
		FileUtils.recursiveDelete(directoryFile);
		directoryFile.mkdirs();
		
		// add a start file
		generateStartFile(destString);
		
		// flush the caches of the tableCores
		flushTheCache();
		
		for(TableCore t: tableCores)
		{
			backupCompletelyTable(t, path, destString);
		}	
		
		generateFinishFile(destString);
	}
	

	 /** Completely backs up of a given tableCore in the database to a destination. It requires the paths for the source and destination databases to be given as arguments
	 * @param t	The tableCore object in the database that needs to be backed up.
	 * @param sourceDatabasePath The source database path.
	 * @param destDatabasePath The destination database path.
	 * @throws FemtoDBIOException
	 */
	private void backupCompletelyTable(TableCore t, String sourceDatabasePath, String destDatabasePath) throws FemtoDBIOException
	{
		generateTableFile(t,destDatabasePath);
		String tableDirectoryString = destDatabasePath + File.separator + Long.toString(t.tableNumber);
		File tableDirectoryFile = new File(tableDirectoryString);
		if(tableDirectoryFile.exists())tableDirectoryFile.delete();
		tableDirectoryFile.mkdirs();
		
		try{
			t.backupCompletely(tableDirectoryString);
		}
		catch(IOException e){ throw new FemtoDBIOException("Database " + name + " IO Exception whilst backing up tableCore " + t.tableNumber, e);}	
	}
	
	private void backupIncrementalTo(String destString) throws FileNotFoundException, FemtoDBIOException
	{
		//TODO
		backupCompletelyTo(destString);
	}
	
	/** Generates a start file in the directory given by the destString argument, removing the older one if it exists. */
	private void generateStartFile(String destString) throws FemtoDBIOException
	{
		String startFileString = destString + File.separator + "start";
		File startFile = new File(startFileString);
		if(startFile.exists())startFile.delete();
		
		OutputStream 		start_os = null;
		ObjectOutputStream 	start_oos = null; 
		try {
			start_os = new FileOutputStream(startFile);
			start_oos = new ObjectOutputStream(start_os);
			start_oos.writeLong(System.currentTimeMillis());
			
		} catch (FileNotFoundException e) {
			throw new FemtoDBIOException("Database " + name + " unable to create the directory to write the start file while backing up: " + startFileString);
		} catch (IOException e) {
			throw new FemtoDBIOException("Database " + name + " was unable to create the following start file while backing up: " + startFileString,e);
		} finally
		{
			if(start_oos != null)
			{
				try {
					start_oos.close();
				} catch (IOException e) {
					throw new FemtoDBIOException("Database " + name + " was unable to close the following start file while backing up: " + startFileString,e);
				}
			}
			if(start_os != null)
			{
				try {
					start_os.close();
				} catch (IOException e) {
					throw new FemtoDBIOException("Database " + name + " was unable to close the following start file while backing up: " + startFileString,e);
				}
			}
			
		}
	}
	
	/** Serialises a given tableCore object into to the directory given by the destString argument. It does not serialise the associated tableCores data files. */
	private void generateTableFile(TableCore t, String destString) throws FemtoDBIOException
	{
		// add a start file
		String tableFileString = destString + File.separator + "tableCore" + t.tableNumber;
		File tableFile = new File(tableFileString);
		OutputStream 		table_os = null;
		ObjectOutputStream 	table_oos = null; 
		try {
			table_os = new FileOutputStream(tableFile);
			table_oos = new ObjectOutputStream(table_os);
			table_oos.writeObject(t);		
		} catch (FileNotFoundException e) {
			throw new FemtoDBIOException("Database " + name + " unable to create the directory to write the tableCore file while backing up: " + tableFileString, e);
		} catch (IOException e) {
			throw new FemtoDBIOException("Database " + name + " was unable to create the following tableCore file while backing up: " + tableFileString, e);
		} finally
		{
			if(table_oos != null)
			{
				try {
					table_oos.close();
				} catch (IOException e) {
					throw new FemtoDBIOException("Database " + name + " was unable to close the following tableCore file while backing up: " + tableFileString, e);
				}
			}
			if(table_os != null)
			{
				try {
					table_os.close();
				} catch (IOException e) {
					throw new FemtoDBIOException("Database " + name + " was unable to close the following tableCore file while backing up: " + tableFileString,e);
				}
			}		
		}
	}
	
	/** Generates a finish file in the directory given by the destString argument, removing the older one if it exists. */
	private void generateFinishFile(String destString) throws FemtoDBIOException
	{
		String finishFileString = destString + File.separator + "finish";
		File finishFile = new File(finishFileString);
		if(finishFile.exists())finishFile.delete();
		
		OutputStream 		finish_os = null;
		ObjectOutputStream 	finish_oos = null; 
		try {
			finish_os = new FileOutputStream(finishFile);
			finish_oos = new ObjectOutputStream(finish_os);
			finish_oos.writeLong(System.currentTimeMillis());
			
		} catch (FileNotFoundException e) {
			throw new FemtoDBIOException("Database " + name + " unable to create the directory to write the finish file while backing up: " + finishFileString);
		} catch (IOException e) {
			throw new FemtoDBIOException("Database " + name + " was unable to create the following finish file while backing up: " + finishFileString,e);
		} finally
		{
			if(finish_oos != null)
			{
				try {
					finish_oos.close();
				} catch (IOException e) {
					throw new FemtoDBIOException("Database " + name + " was unable to close the following finish file while backing up: " + finishFileString,e);
				}
			}
			if(finish_os != null)
			{
				try {
					finish_os.close();
				} catch (IOException e) {
					throw new FemtoDBIOException("Database " + name + " was unable to close the following finish file while backing up: " + finishFileString,e);
				}
			}
			
		}
	}
	
	/** Checks the cache's of all the tableCores and ensures any unsaved modifications are flushed to disk. To maintain performance it does not free any of the cache by marking the cache page as empty.*/
	private void flushTheCache() throws FemtoDBIOException
	{
		for(TableCore t: tableCores)
		{
			t.flushCache();
		}
	}
	
	
	
	// **************************************************
	// **************************************************
	//            CODE FOR LOADING THE DATABASE
	// **************************************************
	// **************************************************	
	
	/** Returns the database at the location given by the path argument if it exists */
	public static FemtoDB open(String path) throws FileNotFoundException, FemtoDBIOException
	{
		String databaseFileString = path + File.separator + "database";	
		File databaseFile = new File(databaseFileString);
		if(!databaseFile.exists()) throw new FileNotFoundException("Unable to find database file " + databaseFileString);

		FemtoDB retval = null;
		
		InputStream is = new FileInputStream(databaseFile);
		try {
			ObjectInputStream ois = new ObjectInputStream(is);
			retval = (FemtoDB) ois.readObject();
			ois.close();
			retval.setPath(path);				
			retval.loadTables();
			return retval;

		} catch (IOException e) {
			throw new FemtoDBIOException("Unable to read database file " + databaseFileString, e);
		} catch (ClassNotFoundException e) {
			throw new FemtoDBIOException("Thats odd, I could not find FemtoDB class when opening database file." + databaseFileString, e);
		}	
	}
	
	/** Returns the database at the location given by the path argument. If it appears to be corrupt or was incorrectly shutdown then
	 * the ping and pong backups present in the directory given by the backup argument are used to attempt a database recovery.  */
	public static FemtoDB open(String path, String backup) throws FileNotFoundException, FemtoDBIOException
	{
		// Open the database if it is not corrupt or was incorrectly shutdown
		long databaseStart = getDatabaseStart(path,true);
		if(databaseStart != -1L)return open(path);
		if(backup == null)throw new FemtoDBIOException("Failed to open the database. The database at " + path + " appears to be corrupt or missing and the backup location given was null");				
		// The database looks corrupt so attempt to recover using the most recent backup
		String pingDirectoryString = backup + File.separator + "ping";
		String pongDirectoryString = backup + File.separator + "ping";
		long pingStart = getDatabaseStart(pingDirectoryString,true);
		long pongStart = getDatabaseStart(pongDirectoryString,true);
		boolean pingOK = (pingStart != -1);
		boolean pongOK = (pongStart != -1);
		File databaseFile = new File(path);
		File pingFile = new File(pingDirectoryString);
		File pongFile = new File(pongDirectoryString);
		
		if( pingOK && pongOK )
		{
			if(pingStart > pongStart)
			{
				// recover using ping			
				try {
					FileUtils.recursiveCopy(databaseFile,pingFile);
					return open(path);				
				} 
				catch (IOException e)
				{
					// ping failed try pong
					try {
						FileUtils.recursiveCopy(databaseFile,pongFile);
						return open(path);
					} catch (IOException e1) {
						throw new FemtoDBIOException("Failed to open the database. The database appears to be corrupt, both the backups appear to be functional but threw IOExceptions whilst copying.", e1);
					}					
				}
			}
			else
			{
				// recover using pong			
				try {
					FileUtils.recursiveCopy(databaseFile,pongFile);
					return open(path);				
				} 
				catch (IOException e)
				{
					// pong failed try ping
					try {
						FileUtils.recursiveCopy(databaseFile,pingFile);
						return open(path);
					} catch (IOException e1) {
						throw new FemtoDBIOException("Failed to open the database. The database appears to be corrupt, both the backups appear to be functional but threw IOExceptions whilst copying.", e1);
					}					
				}
			}//end of pingStart > pongStart if-else
		}// end of pingOK && pongOK if
			
		if(pingOK && (!pongOK))
		{
			// recover using ping only		
			try {
				FileUtils.recursiveCopy(databaseFile,pingFile);
				return open(path);				
			} 
			catch (IOException e)
			{
				throw new FemtoDBIOException("Failed to open the database. The database and pong backup appear to be corrupt, the ping backup appear to be functional but threw IOExceptions whilst copying.", e);				
			}
		}
		
		if((!pingOK) && pongOK)
		{
			// recover using pong only		
			try {
				FileUtils.recursiveCopy(databaseFile,pongFile);
				return open(path);				
			} 
			catch (IOException e)
			{
				throw new FemtoDBIOException("Failed to open the database. The database and ping backup appear to be corrupt, the pong backup appear to be functional but threw IOExceptions whilst copying.", e);				
			}
		}	
		throw new FemtoDBIOException("Failed to open the database. The database and both its backups appear to be corrupted, everythings gone totally foobar. Its time to go get a coffee");
	}
	
	/** Aligns all the paths held in tableCores to the databases current path */ 
	private void loadTables()
	{
		for(TableCore t: tableCores)
		{
			t.finishLoading(this);
		}
	}
	
	/** Returns the start time (in milliseconds) of the database or backup at the given path, or -1 if that backup looks corrupt or did not complete. */
	private static long getDatabaseStart(String path, boolean validateFully)
	{
		String startFileString		= path + File.separator + "start";
		String finishFileString		= path + File.separator + "finish";
		String databaseFileString	= path + File.separator + "database";
		
		File startFile 	= new File(startFileString);
		File finishFile = new File(finishFileString);
		File databaseFile = new File(databaseFileString);
		
		if(!startFile.exists())return -1L;
		if(!finishFile.exists())return -1L;
		if(!databaseFile.exists())return -1L;
		
		FemtoDB backedUpDatabase;
		long start;
		long finish;
		InputStream is1 = null;
		InputStream is2 = null;
		InputStream is3 = null;
		ObjectInputStream ois1 = null;
		ObjectInputStream ois2 = null;
		ObjectInputStream ois3 = null;
		boolean tableReadFault = false;
		try
		{
			is1 = new FileInputStream(startFile);
			ois1 = new ObjectInputStream(is1);
			start = ois1.readLong();
			
			is2 = new FileInputStream(finishFile);
			ois2 = new ObjectInputStream(is2);
			finish = ois2.readLong();
			
			is3 = new FileInputStream(databaseFile);
			ois3 = new ObjectInputStream(is3);
			backedUpDatabase = (FemtoDB)ois3.readObject();
			long temp = 0; // used to force tableCore de-serialisation
			if(backedUpDatabase != null)
			{
					List<TableCore> backedUpTables = backedUpDatabase.tableCores;
					
					// check it is possible to read all the tableCore files
					for(TableCore t: backedUpTables)
					{
						String tableFileString = path + File.separator + "tableCore" + Long.toString(t.tableNumber);
						InputStream isForTable = null;
						ObjectInputStream oisForTable = null;
						TableCore readTable = null;
						try
						{
							isForTable = new FileInputStream(tableFileString);
							oisForTable = new ObjectInputStream(isForTable);
							readTable = (TableCore) oisForTable.readObject();
							temp += readTable.tableNumber;
							if(validateFully)
							{
								if(!readTable.validateTable(path)){tableReadFault = true;}
							}
						}
						catch(Exception e){tableReadFault = true;}
						finally
						{
							if(oisForTable != null)oisForTable.close();
							if(isForTable != null)isForTable.close();
						}
					}
			}
			temp++;
			if((finish < start)||(tableReadFault)||(temp == 0))
			{
				return -1L;
			}
			else
			{
				return start;
			}
		}
		catch(Exception e)
		{
			return -1L;
		}
		finally
		{
			try{
				if(ois1 != null)ois1.close();
				if(ois2 != null)ois2.close();
				if(ois3 != null)ois3.close();
				if(is1 != null)is1.close();
				if(is2 != null)is2.close();
				if(is3 != null)is3.close();
			}
			catch(Exception e){}
		}
	}
	
	// ****************** Locking **********************************
	
	@Override
	public void lock() {databaseLock.lock();}
	
	private void backupLock() {databaseLock.backupLock();}

	/** Blocks until the database lock has been obtained. If the calling thread gets interrupted the method throws an InterruptedException. */ 
	@Override
	public void lockInterruptibly() throws InterruptedException 
	{databaseLock.lockInterruptibly();}

	/** Attempts to acquire the database lock returning immediately. If there are no threads currently holding the database lock then the caller is given the lock and the method will return true. */
	@Override
	public boolean tryLock() {return databaseLock.tryLock();}

	/** Attempts to acquire the database lock with a timeout (using units specified in java.util.concurrent.TimeUnit). The method returns true if the lock was acquired within the timeout.*/
	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException 
	{
		return databaseLock.tryLock(time, unit);
	}

	/** If the calling thread owns the database lock, its hold count is decreased by one. 
	 * If the count reaches zero other threads acquire the lock in the order
	 * they arrived. However if backup or shutdown are called that thread is given priority.
	 */
	@Override
	public void unlock() {databaseLock.unlock();}

	/** Not implemented */
	@Override
	public Condition newCondition() {
		// NOT IMPLEMENTED
		return null;
	}
	
	/** Returns the number of holds on the database lock that the thread that currently holds it has. */
	public final int getHoldCount() {
		return databaseLock.getHoldCount();
	}

	/** Returns the number of threads waiting on the database lock (including the backup thread) */	
	public final int getQueueLength()
	{
		return databaseLock.getQueueLength();
	}

	/** Returns true if the thread given as an argument is waiting for the database lock */	
	public final boolean hasQueuedThread(Thread thread)
	{
		return databaseLock.hasQueuedThread(thread);
	}
	
	/** Returns true if threads are waiting on the database lock */
	public final boolean hasQueuedThreads()	
	{
		return databaseLock.hasQueuedThreads();
	}
	
	/** Returns true if the database lock is currently held by the calling thread */
	public boolean isHeldByCurrentThread()
	{
		return databaseLock.isHeldByCurrentThread();
	}
	
	/** Returns true if a thread currently holds the database lock */
	public boolean isLocked()
	{
		return databaseLock.isLocked();
	}
	

	// ****************** Getters and Setters **********************
	
	/** Returns the path of the database */
	public final String getPath() {
		return path;
	}
	
	/** Returns the name of the database */
	public final String getName() {
		return name;
	}

	/** Sets the path of the database and generates a new start file in that location. It does not set the paths in the databases tableCores */
	public final void setPath(String path) throws FemtoDBIOException {
		this.path = path;
		File pathFile = new File(path);
		if(pathFile.exists())
		{
				generateStartFile(path);
		}
	}


	
	
	
	
	
	
}
