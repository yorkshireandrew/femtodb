package femtodb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class FileCopy {

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
}
