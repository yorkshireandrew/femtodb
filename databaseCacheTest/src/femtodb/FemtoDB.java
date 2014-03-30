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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import femtodbexceptions.FemtoDBIOException;
import femtodbexceptions.FemtoDBShuttingDownException;

/** Implements the databases core functionality, It is responsible for holding the list of tableCore objects as well as providing open, backup and flush the cache functionality. */
public class FemtoDB implements Serializable, Lock {
	private static final long 		serialVersionUID = 1L;
	private String					name;
	private transient String 		path;
	private List<TableCore> 		tableCores;
	private Map<String,TableCore>	tableCoreMap;
	private long 					nextUnusedTableNumber;

	
	private transient DatabaseLock	databaseLock;
	private boolean 				shuttingDown;
	private String					backupDirectory;	
	
	/** Constructs a database core requiring a name as an argument. The setPath method must be also called before the database can be used */
	public FemtoDB(final String name)
	{
		this.name				= name;
		path					= null;
		tableCores 				= new ArrayList<TableCore>();
		tableCoreMap			= new HashMap<String,TableCore>();
		nextUnusedTableNumber 	= 0L;
		databaseLock			= new DatabaseLock();
		shuttingDown			= false;
		backupDirectory			= null;	
	}
	
	/** Obtains the database lock then creates a new tableCore in the database, requiring a name for the tableCore and (optionally) a name for the primary key column 
	 * @throws FemtoDBShuttingDownException */	
	public final TableCore createTable(final String name, final String primaryKeyName) throws FemtoDBShuttingDownException
	{
		lock(); // ensure we have the database lock
		if(shuttingDown)
		{
			unlock();
			throw new FemtoDBShuttingDownException();
		}

		long tableNumber 			= nextUnusedTableNumber++;
		TableCore newTable 			= new TableCore(this, name, tableNumber, primaryKeyName);
		tableCores.add(newTable);
		tableCoreMap.put(name, newTable);	
		unlock();
		return newTable;
	}
	
	/** Obtains a lock on the database then returns the named tableCore */
	public final TableCore getTable(final String name) throws FemtoDBShuttingDownException
	{
		lock(); // ensure we have the database lock
		if(shuttingDown)
		{
			unlock();
			throw new FemtoDBShuttingDownException();
		}
		TableCore retval = tableCoreMap.get(name);
		unlock();
		return retval;
	}
	
	/** Obtains the database lock then creates a new tableCore in the database, requiring a name for the tableCore and (optionally) a name for the primary key column 
	 * @throws FemtoDBShuttingDownException 
	 * @throws FemtoDBIOException */	
	public final void deleteTable(final String name) throws FemtoDBShuttingDownException, FemtoDBIOException
	{
		lock(); // ensure we have the database lock
		if(shuttingDown)
		{
			unlock();
			throw new FemtoDBShuttingDownException();
		}
		
		TableCore t = tableCoreMap.get(name);
		if(t != null)
		{
			tableCores.remove(t);
			tableCoreMap.remove(t);
			try{
				t.deleteTable(path);
			}
			catch(FemtoDBIOException e)
			{
				unlock();
				throw e;
			}
		}		
		unlock();
	}
	
	/** Shuts down the database 
	 * @throws FemtoDBIOException */
	public final void shutdown() throws FemtoDBIOException 
	{
		if(shuttingDown)return;
		
		// acquire the database lock as a high priority
		databaseLock.shutdownLock();
		shuttingDown = true;
		if(path == null)return;
		
		try{
			generateDatabaseFile(path);
			for(TableCore t: tableCores)
			{
				t.shutdownTable();
			}
			
			// generate a finish file that indicates the shutdown completed
			generateFinishFile(path);
		}
		catch(FemtoDBIOException e)
		{
			throw e;
		}
		finally
		{
			databaseLock.unlock();
		}
	}
	
	/** Obtains the database lock then creates a backup of the database at its backupDirectory. The method automatically creates ping and pong subdirectories if they do not exist. If a backup exists the method will overwrite the oldest (or the most invalid) ping or pong backup. 
	 * @throws FemtoDBShuttingDownException */
	public final void backup()  throws FemtoDBIOException, FemtoDBShuttingDownException
	{
		// we can only proceed once we hold the database lock
		lock();
		try{
			backupInternal();
		}
		catch(FemtoDBIOException e)
		{
			throw e;
		}
		catch(FileNotFoundException e)
		{
			throw new FemtoDBIOException("Could not find file",e);
		}
		catch(FemtoDBShuttingDownException e)
		{
			throw e;
		}
		finally
		{
			unlock();
		}
	}
		
	/** Creates a backup of the database at its backupDirectory. The method automatically creates ping and pong subdirectories if they do not exist. If a backup exists the method will overwrite the oldest (or the most invalid) ping or pong backup. */
	private final void backupInternal() throws FemtoDBShuttingDownException, FemtoDBIOException, FileNotFoundException
	{
		// check we are not shutting down
		if(shuttingDown)throw new FemtoDBShuttingDownException();
		
		// ensure the path exists
		File backupLocationFile = new File(backupDirectory);
		if(!backupLocationFile.exists())backupLocationFile.mkdirs(); 
		
		// ensure there is a ping directory
		String pingDirectoryString = backupDirectory + File.separator + "ping";
		File ping = new File(pingDirectoryString);
		if(!ping.exists())
		{
			if(!ping.mkdir()) throw new FemtoDBIOException("Database " + name + " was unable to create the following directory when backing up: " + pingDirectoryString);
		}
		
		// ensure there is a pong directory
		String pongDirectoryString = backupDirectory + File.separator + "pong";
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
	
	private final void backupCompletelyTo(final String destDirectory) throws FileNotFoundException, FemtoDBIOException
	{
		if(path == null)throw new FemtoDBIOException("Database " + name + " backup was attempted before the database path was set");
		// remove the old directory if it exists and generate a new one
		File directoryFile = new File(destDirectory);
		FileUtils.recursiveDelete(directoryFile);
		directoryFile.mkdirs();
		
		// add a start file
		generateStartFile(destDirectory);
		
		// add the database file
		generateDatabaseFile(destDirectory);
		
		for(TableCore t: tableCores)
		{
			t.backupCompletely(destDirectory);
		}	
		
		generateFinishFile(destDirectory);
	}
	
	private final void backupIncrementalTo(final String destString) throws FileNotFoundException, FemtoDBIOException
	{
		//TODO
		backupCompletelyTo(destString);
	}
	
	/** Generates a start file in the directory given by the destString argument, removing the older one if it exists. */
	private final void generateStartFile(final String destString) throws FemtoDBIOException
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
	
	/** Generates a database file in the directory given by the destString argument, removing the older one if it exists. */
	private final void generateDatabaseFile(final String destString) throws FemtoDBIOException
	{
		String databaseFileString = destString + File.separator + "database";
		File databaseFile = new File(databaseFileString);
		if(databaseFile.exists())databaseFile.delete();
		
		OutputStream 		database_os = null;
		ObjectOutputStream 	database_oos = null; 
		try {
			database_os = new FileOutputStream(databaseFile);
			database_oos = new ObjectOutputStream(database_os);
			database_oos.writeObject(this);		
		} catch (FileNotFoundException e) {
			throw new FemtoDBIOException("Database " + name + " unable to create the directory to write the database file while backing up: " + databaseFileString);
		} catch (IOException e) {
			throw new FemtoDBIOException("Database " + name + " was unable to create the following database file while backing up: " + databaseFileString,e);
		} finally
		{
			if(database_oos != null)
			{
				try {
					database_oos.close();
				} catch (IOException e) {
					throw new FemtoDBIOException("Database " + name + " was unable to close the following start file while backing up: " + databaseFileString,e);
				}
			}
			if(database_os != null)
			{
				try {
					database_os.close();
				} catch (IOException e) {
					throw new FemtoDBIOException("Database " + name + " was unable to close the following start file while backing up: " + databaseFileString,e);
				}
			}
			
		}
	}
	
	/** Generates a finish file in the directory given by the destString argument, removing the older one if it exists. */
	private final void generateFinishFile(final String destString) throws FemtoDBIOException
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
	
	// **************************************************
	// **************************************************
	//            CODE FOR LOADING THE DATABASE
	// **************************************************
	// **************************************************	
	
	/** Returns the database at the location given by the path argument if it exists */
	private final static FemtoDB openInternal(final String path, final String backupDirectory) throws FileNotFoundException, FemtoDBIOException
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
			retval.setBackupDirectory(backupDirectory);
			retval.loadTables();
			retval.databaseLock = new DatabaseLock();
			retval.shuttingDown = false;
			return retval;

		} catch (IOException e) {
			throw new FemtoDBIOException("Unable to read database file " + databaseFileString, e);
		} catch (ClassNotFoundException e) {
			try {is.close();} catch (IOException e1) {}
			throw new FemtoDBIOException("Thats odd, I could not find FemtoDB class when opening database file." + databaseFileString, e);
		}	
	}
	
	public final void setBackupDirectory(final String backupDirectory) {
		this.backupDirectory = backupDirectory;	
	}

	/** Returns the database at the location given by the path argument. If it appears to be corrupt or was incorrectly shutdown then
	 * the ping and pong backups present in the directory given by the backup argument are used to attempt a database recovery.  */
	public final static FemtoDB open(final String path, final String backupDirectory) throws FileNotFoundException, FemtoDBIOException
	{
		// Open the database if it is not corrupt or was incorrectly shutdown
		long databaseStart = getDatabaseStart(path,true);
		if(databaseStart != -1L)System.out.println("Database appears to be fine with start number " + databaseStart);
		if(databaseStart != -1L)return openInternal(path,backupDirectory);
		if(backupDirectory == null)throw new FemtoDBIOException("Failed to open the database. The database at " + path + " appears to be corrupt or missing and the backup location given was null");				
	
		// The database looks corrupt so attempt to recover using the most recent backup
		String pingDirectoryString = backupDirectory + File.separator + "ping";
		String pongDirectoryString = backupDirectory + File.separator + "pong";
		long pingStart = getDatabaseStart(pingDirectoryString,true);
		long pongStart = getDatabaseStart(pongDirectoryString,true);
		boolean pingOK = (pingStart != -1);
		boolean pongOK = (pongStart != -1);
		File databaseFile = new File(path);
		File pingFile = new File(pingDirectoryString);
		File pongFile = new File(pongDirectoryString);
		
		System.out.println("Database appears to be corrupt opening using a backup");
		if(pingOK)System.out.println("Ping is OK");
		System.out.println("Ping start is " + pingStart);
		if(pongOK)System.out.println("Pong is OK");
		System.out.println("Pong start is " + pongStart);
		
		if( pingOK && pongOK )
		{
			if(pingStart > pongStart)
			{
				// recover using ping			
				try {
					FileUtils.recursiveCopy(pingFile,databaseFile);
					return openInternal(path,backupDirectory);				
				} 
				catch (IOException e)
				{
					// ping failed try pong
					try {
						FileUtils.recursiveCopy(pongFile,databaseFile);
						return openInternal(path,backupDirectory);
					} catch (IOException e1) {
						throw new FemtoDBIOException("Failed to open the database. The database appears to be corrupt, both the backups appear to be functional but threw IOExceptions whilst copying.", e1);
					}					
				}
			}
			else
			{
				// recover using pong	
				System.out.println(" recover using pong ");
				try {
					FileUtils.recursiveCopy(pongFile,databaseFile);
					return openInternal(path,backupDirectory);				
				} 
				catch (IOException e)
				{
					System.out.println(e);
					e.printStackTrace();
					System.out.println(" pong failed trying ping ");
					// pong failed try ping
					try {
						FileUtils.recursiveCopy(pingFile,databaseFile);
						return openInternal(path,backupDirectory);
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
				FileUtils.recursiveCopy(pingFile,databaseFile);
				return openInternal(path,backupDirectory);				
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
				FileUtils.recursiveCopy(pongFile,databaseFile);
				return openInternal(path,backupDirectory);				
			} 
			catch (IOException e)
			{
				throw new FemtoDBIOException("Failed to open the database. The database and ping backup appear to be corrupt, the pong backup appear to be functional but threw IOExceptions whilst copying.", e);				
			}
		}	
		throw new FemtoDBIOException("Failed to open the database. The database and both its backups appear to be corrupted, everythings gone totally foobar. Its time to go get a coffee");
	}
	
	/** Aligns all the paths held in tableCores to the databases current path */ 
	private final void loadTables()
	{
		for(TableCore t: tableCores)
		{
			t.finishLoading(this);
		}
	}
	
	/** Returns the start time (in milliseconds) of the database or backup at the given path, or -1 if that backup looks corrupt or did not complete. */
	private static final long getDatabaseStart(final String path, final boolean validateFully)
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
						String tableFileString = path + File.separator + "tableCore" + Long.toString(t.getTableNumber());
						InputStream isForTable = null;
						ObjectInputStream oisForTable = null;
						TableCore readTable = null;
						try
						{
							isForTable = new FileInputStream(tableFileString);
							oisForTable = new ObjectInputStream(isForTable);
							readTable = (TableCore) oisForTable.readObject();
							temp += readTable.getTableNumber();
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
	public final void setPath(final String path) throws FemtoDBIOException {
		this.path = path;
		File pathFile = new File(path);
		if(!pathFile.exists())pathFile.mkdirs();
		generateStartFile(path);
	}
	
	
	
	// ****************** Locking **********************************
	
	@Override
	public final void lock() {databaseLock.lock();}

	/** Blocks until the database lock has been obtained. If the calling thread gets interrupted the method throws an InterruptedException. */ 
	@Override
	public final void lockInterruptibly() throws InterruptedException 
	{databaseLock.lockInterruptibly();}

	/** Attempts to acquire the database lock returning immediately. If there are no threads currently holding the database lock then the caller is given the lock and the method will return true. */
	@Override
	public final boolean tryLock() {return databaseLock.tryLock();}

	/** Attempts to acquire the database lock with a timeout (using units specified in java.util.concurrent.TimeUnit). The method returns true if the lock was acquired within the timeout.*/
	@Override
	public final boolean tryLock(long time, TimeUnit unit) throws InterruptedException 
	{
		return databaseLock.tryLock(time, unit);
	}

	/** If the calling thread owns the database lock, its hold count is decreased by one. 
	 * If the count reaches zero other threads acquire the lock in the order
	 * they arrived. However if backup or shutdown are called that thread is given priority.
	 */
	@Override
	public final void unlock() {databaseLock.unlock();}

	/** Not implemented */
	@Override
	public final Condition newCondition() {
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
	public final boolean isHeldByCurrentThread()
	{
		return databaseLock.isHeldByCurrentThread();
	}
	
	/** Returns true if a thread currently holds the database lock */
	public final boolean isLocked()
	{
		return databaseLock.isLocked();
	}


	
	
	
	
	
	
}
