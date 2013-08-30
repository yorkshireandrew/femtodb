package databasecachetest;

import femtodbexceptions.CharArrayExceedsColumnSizeException;
import femtodbexceptions.StringExceedsColumnSizeException;

public class BuffWrite {

		static byte[] writeBuffer2 = new byte[8];
		
		
		/** writes one byte value into buffer at the given offset */
	    static final void write(final byte[] buff, final int offset, final int value)
	    {
	    	buff[offset] = (byte)value;
	    }
	    
		/** writes a series of bytes from a source array into buffer at the given offset
	     * @param buff Buffer to be written into
	     * @param offset Offset into the buffer to commence writing
	     * @param src Source byte array
	     * @param srcPos Position in source array to start reading from
	     * @param length Number of bytes to copy into the buffer
	     */
	    static final void writeBytes(final byte[] buff, final int offset, final byte[] src, final int srcPos, final int length)
	    {
	    	System.arraycopy(src, srcPos, buff, offset, length);
	    }
	    
		/** writes a byte array (including 4 byte length information) into buffer at the given offset
	     * @param buff Buffer to be written into
	     * @param offset Offset into the buffer to commence writing
	     * @param src Source byte array
	     */
	    static final void writeBytes(final byte[] buff, int offset, final byte[] src)
	    {
	    	int length = src.length;
	    	writeInt(buff,offset,src.length);
	    	offset += 4;
	    	System.arraycopy(src, 0, buff, offset, length);
	    }
	    
	    static final void writeBoolean(final byte[] buff, final int offset, final boolean v) 
	    {
			write(buff, offset, (v ? 1 : 0));
	    }

	    static final void writeShort(final byte[] buff, int offset, final int v)
	    {
	        write(buff, offset++,((v >>> 8) & 0xFF));
	        write(buff, offset,((v >>> 0) & 0xFF));
	    }
	    
	    static final void writeChar(final byte[] buff, int offset, final int v)
	    {
	        write(buff, offset++,((v >>> 8) & 0xFF));
	        write(buff, offset,((v >>> 0) & 0xFF));
	    }

	    static final void writeInt(final byte[] buff, int offset, final int v)
	    {
	        write(buff, offset++, ((v >>> 24) & 0xFF));
	        write(buff, offset++, ((v >>> 16) & 0xFF));
	        write(buff, offset++, ((v >>>  8) & 0xFF));
	        write(buff, offset, ((v >>>  0) & 0xFF));
	    }

	    static final void writeLong(final byte[] buff, final int offset, final long v)
	    {
	    	byte writeBuffer[] = new byte[8];
	        writeBuffer[0] = (byte)(v >>> 56);
	        writeBuffer[1] = (byte)(v >>> 48);
	        writeBuffer[2] = (byte)(v >>> 40);
	        writeBuffer[3] = (byte)(v >>> 32);
	        writeBuffer[4] = (byte)(v >>> 24);
	        writeBuffer[5] = (byte)(v >>> 16);
	        writeBuffer[6] = (byte)(v >>>  8);
	        writeBuffer[7] = (byte)(v >>>  0);
	        writeBytes(buff, offset, writeBuffer, 0, 8);
	    }
	    
	    static final void writeLong2(final byte[] buff, final int offset, final long v)
	    {
	    	byte[] writeBuffer2L = writeBuffer2;
	        writeBuffer2L[0] = (byte)(v >>> 56);
	        writeBuffer2L[1] = (byte)(v >>> 48);
	        writeBuffer2L[2] = (byte)(v >>> 40);
	        writeBuffer2L[3] = (byte)(v >>> 32);
	        writeBuffer2L[4] = (byte)(v >>> 24);
	        writeBuffer2L[5] = (byte)(v >>> 16);
	        writeBuffer2L[6] = (byte)(v >>>  8);
	        writeBuffer2L[7] = (byte)(v >>>  0);
	        writeBytes(buff, offset, writeBuffer2L, 0, 8);
	    }
	    
	    static final void writeFloat(final byte[] buff, final int offset, final float v){
	    	writeInt(buff, offset, Float.floatToIntBits(v));
	    }
	    
	    static final void writeDouble(final byte[] buff, final int offset, final double v){
	    	writeLong(buff, offset,Double.doubleToLongBits(v));
	    }
	    
	    /** Writes a character array to the buffer (including a 2 byte length value). The length must not exceed 65535 characters */
	    static final void writeChars(final byte[] buff, int offset, final char[] chars) throws CharArrayExceedsColumnSizeException
	    {
	    	int len = chars.length;
	    	if(len > 65535)throw new CharArrayExceedsColumnSizeException();
	    	writeShort(buff,offset,len);
	    	offset += 2;
	    	for(int x = 0; x < len; x++)
	    	{
	    		writeChar(buff,offset,chars[x]);
	    		offset += 2;
	    	} 	
	    }
	    
	    static final void writeString(final byte[] buff, final int offset, final String str) throws StringExceedsColumnSizeException {
	        int strlen = str.length();
	        int utflen = 0;
	        int c, count = 0;
	 
	        /* use charAt instead of copying String to char array */
	        for (int i = 0; i < strlen; i++) 
	        {
		        c = str.charAt(i);
			    if ((c >= 0x0001) && (c <= 0x007F)) {
				utflen++;
			    } else if (c > 0x07FF) {
				utflen += 3;
			    } else {
				utflen += 2;
			    }
	        }

	        if (utflen > 65535)throw new StringExceedsColumnSizeException();

	        byte[] bytearr = new byte[utflen+2];
	     
	        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
	        bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);  
	        
	        int i=0;
	        for (i=0; i<strlen; i++) {
	           c = str.charAt(i);
	           if (!((c >= 0x0001) && (c <= 0x007F))) break;
	           bytearr[count++] = (byte) c;
	        }
		
			for (;i < strlen; i++)
			{
		            c = str.charAt(i);
			    if ((c >= 0x0001) && (c <= 0x007F)) {
				bytearr[count++] = (byte) c;
		               
			    } else if (c > 0x07FF) {
				bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
				bytearr[count++] = (byte) (0x80 | ((c >>  6) & 0x3F));
				bytearr[count++] = (byte) (0x80 | ((c >>  0) & 0x3F));
			    } else {
				bytearr[count++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
				bytearr[count++] = (byte) (0x80 | ((c >>  0) & 0x3F));
			    }
			}
		    
			writeBytes(buff, offset, bytearr, 0, utflen+2);
	    }

}
