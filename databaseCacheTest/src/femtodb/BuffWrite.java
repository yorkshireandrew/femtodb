package femtodb;

import femtodbexceptions.FemtoBDCharArrayExceedsColumnSizeException;
import femtodbexceptions.FemtoDBStringExceedsColumnSizeException;

/** Writes primitive types or a source byte array into a data byte array starting at a given offset */
public class BuffWrite {

		private static byte[] writeBuffer2 = new byte[8];
		
		
		/** writes one byte value into the data byte array at the given offset */
	    static final void write(final byte[] data, final int offset, final int value)
	    {
	    	data[offset] = (byte)value;
	    }
	    
		/** writes a series of bytes from a source array into buffer at the given offset
	     * @param data byte array to be written into
	     * @param offset Offset into the data byte array to commence writing
	     * @param src Source byte array
	     * @param srcPos Position in source array to start reading from
	     * @param length Number of bytes to copy into the data array
	     */
	    static final void writeBytes(final byte[] data, final int offset, final byte[] src, final int srcPos, final int length)
	    {
	    	System.arraycopy(src, srcPos, data, offset, length);
	    }
	    
		/** writes a byte array (including 4 byte length information) into a data byte array starting at the given offset
	     * @param data data byte array to be written into
	     * @param offset Offset into the data byte array to commence writing
	     * @param src Source byte array
	     */
	    static final void writeBytes(final byte[] data, int offset, final byte[] src)
	    {
	    	int length = src.length;
	    	writeInt(data,offset,src.length);
	    	offset += 4;
	    	System.arraycopy(src, 0, data, offset, length);
	    }
	    
	    /** Writes a boolean (represented as a single byte) into a data byte array at a given offset */
	    static final void writeBoolean(final byte[] data, final int offset, final boolean v) 
	    {
			write(data, offset, (v ? 1 : 0));
	    }

	    /** Writes a short into a data byte array at a given offset */
	    static final void writeShort(final byte[] data, int offset, final int v)
	    {
	        write(data, offset++,((v >>> 8) & 0xFF));
	        write(data, offset,((v >>> 0) & 0xFF));
	    }

	    /** Writes a char into a data byte array at a given offset */   
	    static final void writeChar(final byte[] data, int offset, final int v)
	    {
	        write(data, offset++,((v >>> 8) & 0xFF));
	        write(data, offset,((v >>> 0) & 0xFF));
	    }

	    /** Writes an integer into a data byte array at a given offset */	    
	    static final void writeInt(final byte[] data, int offset, final int v)
	    {
	        write(data, offset++, ((v >>> 24) & 0xFF));
	        write(data, offset++, ((v >>> 16) & 0xFF));
	        write(data, offset++, ((v >>>  8) & 0xFF));
	        write(data, offset, ((v >>>  0) & 0xFF));
	    }
	    
	    /** Writes a long into a data byte array at a given offset */
	    synchronized
	    static final void writeLong(final byte[] data, final int offset, final long v)
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
	        writeBytes(data, offset, writeBuffer2L, 0, 8);
	    }

	    /** Writes a float into a data byte array at a given offset */ 
	    static final void writeFloat(final byte[] data, final int offset, final float v){
	    	writeInt(data, offset, Float.floatToIntBits(v));
	    }

	    /** Writes a double into a data byte array at a given offset */    
	    static final void writeDouble(final byte[] data, final int offset, final double v){
	    	writeLong(data, offset,Double.doubleToLongBits(v));
	    }
	    
	    /** Writes a character array to the data byte array (including a 2 byte length value). The length must not exceed 65535 characters */
	    static final void writeChars(final byte[] data, int offset, final char[] chars, int limit) throws FemtoBDCharArrayExceedsColumnSizeException
	    {
	    	int len = chars.length;
	        if ((len*2 + 2) > limit)throw new FemtoBDCharArrayExceedsColumnSizeException();
	    	if(len > 65535)throw new FemtoBDCharArrayExceedsColumnSizeException();
	    	writeShort(data,offset,len);
	    	offset += 2;
	    	for(int x = 0; x < len; x++)
	    	{
	    		writeChar(data,offset,chars[x]);
	    		offset += 2;
	    	} 	
	    }
	    
	    /** Writes a string (using modified UTF format) into a data byte array at a given offset. If the length of the bytes to be written exceeds the length argument
	     *  or 65535 then a FemtoDBStringExceedsColumnSizeException is throw. */ 
	    static final void writeString(final byte[] data, final int offset, final int limit, final String string) throws FemtoDBStringExceedsColumnSizeException {
	        int strlen = string.length();
	        int utflen = 0;
	        int c, count = 0;
	 
	        /* use charAt instead of copying String to char array */
	        for (int i = 0; i < strlen; i++) 
	        {
		        c = string.charAt(i);
			    if ((c >= 0x0001) && (c <= 0x007F)) {
				utflen++;
			    } else if (c > 0x07FF) {
				utflen += 3;
			    } else {
				utflen += 2;
			    }
	        }

	        if ((utflen + 2) > limit)throw new FemtoDBStringExceedsColumnSizeException();
	        if (utflen > 65535)throw new FemtoDBStringExceedsColumnSizeException();

	        byte[] bytearr = new byte[utflen+2];
	     
	        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
	        bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);  
	        
	        int i=0;
	        for (i=0; i<strlen; i++) {
	           c = string.charAt(i);
	           if (!((c >= 0x0001) && (c <= 0x007F))) break;
	           bytearr[count++] = (byte) c;
	        }
		
			for (;i < strlen; i++)
			{
		            c = string.charAt(i);
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
		    
			writeBytes(data, offset, bytearr, 0, utflen+2);
	    }

}
