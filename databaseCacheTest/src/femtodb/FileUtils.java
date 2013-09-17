package femtodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class FileUtils {

	public static void copyFile(File source, File dest) throws IOException
	{
		FileInputStream fis = null;
		FileOutputStream fos = null;
		
		try{
			fis = new FileInputStream(source);
			fos = new FileOutputStream(dest);
			FileChannel fis_channel = fis.getChannel();
			WritableByteChannel fos_channel = fos.getChannel();
			long num = fis_channel.size();
			fis_channel.transferTo(0, num, fos_channel);		
		}
		catch(IOException e)
		{
			throw new IOException("IOException copying " + source.getAbsolutePath() + " to " + dest.getAbsolutePath(), e);
		}
		finally
		{
			if(fis != null)fis.close();
			if(fos != null)fos.close();
		}	
	}
	
	/** Copies a file or directory given by the source argument, 
	 * to the file or directory given in the destination argument. 
	 * If the destination directory already exists then its contents 
	 * are deleted before the copy occurs. */
	public static void recursiveCopy(File source, File dest) throws IOException
	{
		if(!source.exists())return;
		if(dest.exists())recursiveDelete(dest);
		recursiveCopy2(source,dest);
	}
	
	private static void recursiveCopy2(File source, File dest) throws IOException
	{
		if(source.isDirectory())
		{
			dest.mkdirs();			
			File[] subs = source.listFiles();
			for(File f: subs)
			{
				String name = f.getName();
				File saveTo = new File(dest,name);
				recursiveCopy(f,saveTo);
			}
		}
		else
		{
			copyFile(source,dest);
		}	
	}
	
	/** Recursively deletes a file or directory */
	public static void recursiveDelete(File f)
	{
		if(f != null)
		{
			if(f.exists())
			{
				if(f.isDirectory())
				{
					File[] files = f.listFiles();
					for(File file: files)
					{
						recursiveDelete(file);
					}
					f.delete();
				}
				else
				{
					f.delete();
				}
			}
		}
	}
}
