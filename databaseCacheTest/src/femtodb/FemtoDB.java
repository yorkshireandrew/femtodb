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

import femtodbexceptions.FemtoDBIOException;

public class FemtoDB implements Serializable{
	private static final long serialVersionUID = 1L;
	private String				name;
	private transient String 	path;
	private List<Table> 		tables;
	private long 				nextUnusedTableNumber;
	
	public FemtoDB(String name)
	{
		this.name				= name;
		path					= null;
		tables 					= new ArrayList<Table>();
		nextUnusedTableNumber 	= 0L;
	}
	
	public Table createTable(String name, String primaryKeyName)
	{
		long tableNumber 	= nextUnusedTableNumber++;
		Table newTable 		= new Table(this, name, tableNumber, primaryKeyName);
		tables.add(newTable);
		return newTable;
	}
	

	
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
		long 	validPingDatabaseStart = getDatabaseStart(pingDirectoryString);	
		long 	validPongDatabaseStart = getDatabaseStart(pongDirectoryString);
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
		Table.recursiveDelete(directoryFile);
		directoryFile.mkdirs();
		
		// add a start file
		generateStartFile(destString);
		
		// flush the caches of the tables
		flushTheCache();
		
		for(Table t: tables)
		{
			backupCompletelyTable(t, path, destString);
		}	
		
		generateFinishFile(destString);
	}
	
	private void backupCompletelyTable(Table t, String pathToDatabase, String destPath) throws FemtoDBIOException
	{
		generateTableFile(t,destPath);
		String tableDirectoryString = destPath + File.separator + Long.toString(t.tableNumber);
		File tableDirectoryFile = new File(tableDirectoryString);
		if(tableDirectoryFile.exists())tableDirectoryFile.delete();
		tableDirectoryFile.mkdirs();
		
		try{
			t.backupFully(tableDirectoryString);
		}
		catch(IOException e){ throw new FemtoDBIOException("Database " + name + " IO Exception whilst backing up table " + t.tableNumber, e);}	
	}
	
	private void backupIncrementalTo(String destString) throws FileNotFoundException, FemtoDBIOException
	{
		//TODO
		backupCompletelyTo(destString);
	}
	
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
	
	private void generateTableFile(Table t, String destString) throws FemtoDBIOException
	{
		// add a start file
		String tableFileString = destString + File.separator + "table" + t.tableNumber;
		File tableFile = new File(tableFileString);
		OutputStream 		table_os = null;
		ObjectOutputStream 	table_oos = null; 
		try {
			table_os = new FileOutputStream(tableFile);
			table_oos = new ObjectOutputStream(table_os);
			table_oos.writeObject(t);		
		} catch (FileNotFoundException e) {
			throw new FemtoDBIOException("Database " + name + " unable to create the directory to write the table file while backing up: " + tableFileString, e);
		} catch (IOException e) {
			throw new FemtoDBIOException("Database " + name + " was unable to create the following table file while backing up: " + tableFileString, e);
		} finally
		{
			if(table_oos != null)
			{
				try {
					table_oos.close();
				} catch (IOException e) {
					throw new FemtoDBIOException("Database " + name + " was unable to close the following table file while backing up: " + tableFileString, e);
				}
			}
			if(table_os != null)
			{
				try {
					table_os.close();
				} catch (IOException e) {
					throw new FemtoDBIOException("Database " + name + " was unable to close the following table file while backing up: " + tableFileString,e);
				}
			}		
		}
	}
	
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
	
	private void flushTheCache() throws FemtoDBIOException
	{
		for(Table t: tables)
		{
			t.flushCache();
		}
	}
	
	
	
	// **************************************************
	// **************************************************
	//            CODE FOR LOADING THE DATABASE
	// **************************************************
	// **************************************************	
	
	static FemtoDB open(String path) throws FileNotFoundException, FemtoDBIOException
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
			retval.generateStartFile(path);

		} catch (IOException e) {
			throw new FemtoDBIOException("Unable to read database file " + databaseFileString, e);
		} catch (ClassNotFoundException e) {
			throw new FemtoDBIOException("Thats odd, I could not find FemtoDB class when opening database file." + databaseFileString, e);
		}	

		retval.generateStartFile(path);
		return retval;
	}
	
	private void loadTables()
	{
		for(Table t: tables)
		{
			t.finishLoading(this);
		}
	}
	
	private long getDatabaseStart(String path)
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
			long temp = 0;
			if(backedUpDatabase != null)
			{
					List<Table> backedUpTables = backedUpDatabase.tables;
					
					// check it is possible to read all the table files
					for(Table t: backedUpTables)
					{
						String tableFileString = path + File.separator + "table" + t.tableNumber;
						InputStream isForTable = null;
						ObjectInputStream oisForTable = null;
						Table readTable = null;
						try
						{
							isForTable = new FileInputStream(tableFileString);
							oisForTable = new ObjectInputStream(isForTable);
							readTable = (Table) oisForTable.readObject();
							temp += readTable.tableNumber;
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
	
	public final String getPath() {
		return path;
	}

	public final void setPath(String path) throws FemtoDBIOException {
		this.path = path;
		File pathFile = new File(path);
		if(pathFile.exists())
		{
				generateStartFile(path);
		}
	}
	
	
	
	
}
