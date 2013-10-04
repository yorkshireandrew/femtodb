package femtodb;

import java.io.UTFDataFormatException;

/** Reads primitive types or a byte array from a data byte array starting at a given offset */
public class BuffRead {

	static byte[] readBuffer2 = new byte[8];
	/** read a single byte */
    static final int read(final byte[] data, final int offset)
    {
    	return data[offset] & 0xFF;
    }
    
    static final void readFully(final byte[] data, final int offset, final byte[] dest, final int destPos, final int len)
    {
    	
    	System.arraycopy(data, offset, dest, destPos, len);
    }
    
    static final byte[] readByteArray(final byte[] data, int offset)
    {
    	int len = readInt(data,offset);
    	offset += 4;
    	byte[] retval = new byte[len];
    	for(int x = 0; x < len ; x++)
    	{
    		retval[x] = (byte)read(data,offset++);
    	}
    	return retval;
    }

    static final boolean readBoolean(final byte[] data, final int offset)
    {
    	byte ch = data[offset];
    	return (ch != 0);
    }
    
    static final short readShort(final byte[] data, int offset)
    {
        int ch1 = data[offset++] & 0xFF;
        int ch2 = data[offset] & 0xFF;
        return (short)((ch1 << 8) + (ch2 << 0));
    }

    static final char readChar(final byte[] data, int offset)
    {
        int ch1 = data[offset++] & 0xFF;
        int ch2 = data[offset] & 0xFF;
        return (char)((ch1 << 8) + (ch2 << 0));
    }

    static final int readInt(final byte[] data, int offset)
    {
        int ch1 = data[offset++] & 0xFF;
        int ch2 = data[offset++] & 0xFF;
        int ch3 = data[offset++] & 0xFF;
        int ch4 = data[offset] & 0xFF;
        return ((ch1 << 24) | (ch2 << 16) | (ch3 << 8) | (ch4 << 0));
    }
   
    synchronized
    static final long readLong(final byte[] data, int offset)
    {
    	byte[] readBuffer2L = readBuffer2;
        readFully(data, offset, readBuffer2L, 0, 8);
        return (
        		((long)(readBuffer2L[0] & 255) << 56) |
                ((long)(readBuffer2L[1] & 255) << 48) |
                ((long)(readBuffer2L[2] & 255) << 40) |
                ((long)(readBuffer2L[3] & 255) << 32) |
                ((long)(readBuffer2L[4] & 255) << 24) |
                ((readBuffer2L[5] & 255) << 16) |
                ((readBuffer2L[6] & 255) <<  8) |
                ((readBuffer2L[7] & 255) <<  0));
    }

    static final float readFloat(final byte[] data, final int offset){
    	return Float.intBitsToFloat(readInt(data, offset));
    }


    static final double readDouble(final byte[] data, final int offset){
    	return Double.longBitsToDouble(readLong(data, offset));
    }
    
    static final char[] readCharArray(final byte[] data, int offset)
    {
    	int len = readShort(data,offset);
    	offset += 2;
    	char[] retval = new char[len];
    	for(int x = 0; x < len; x++)
    	{
    		retval[x] = readChar(data,offset);
    		offset += 2;
    	}
    	return retval;
    }
    /** Returns a String representing the string data that was encoded into the data byte array starting at a given offset. */
    static final String readString(final byte[] data, int offset) throws UTFDataFormatException {
    	int utflen = readShort(data, offset);
        offset += 2;
        char[] chararr = new char[utflen];

        int c, char2, char3;
        int count = 0;
        int chararr_count=0;

        while (count < utflen) {
            c = (int) data[offset + count] & 0xff;      
            if (c > 127) break;
            count++;
            chararr[chararr_count++]=(char)c;
        }

        while (count < utflen) {
            c = (int) data[offset + count] & 0xff;
            switch (c >> 4) {
                case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                    /* 0xxxxxxx*/
                    count++;
                    chararr[chararr_count++]=(char)c;
                    break;
                case 12: case 13:
                    /* 110x xxxx   10xx xxxx*/
                    count += 2;
                    if (count > utflen)
                        throw new UTFDataFormatException(
                            "malformed input: partial character at end");
                    char2 = (int) data[offset + count-1];
                    if ((char2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException(
                            "malformed input around byte " + count); 
                    chararr[chararr_count++]=(char)(((c & 0x1F) << 6) | 
                                                    (char2 & 0x3F));  
                    break;
                case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    count += 3;
                    if (count > utflen)
                        throw new UTFDataFormatException(
                            "malformed input: partial character at end");
                    char2 = (int) data[offset + count-2];
                    char3 = (int) data[offset + count-1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw new UTFDataFormatException(
                            "malformed input around byte " + (count-1));
                    chararr[chararr_count++]=(char)(((c     & 0x0F) << 12) |
                                                    ((char2 & 0x3F) << 6)  |
                                                    ((char3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx,  1111 xxxx */
                    throw new UTFDataFormatException(
                        "malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararr_count);
    }
    
    /** Modifies and returns the passed in string builder so that it contain the string data encoded into the data byte array starting at a given offset. */
    static final StringBuilder readStringBuilder(final byte[] data, int offset, StringBuilder stringBuilder) throws UTFDataFormatException {
    	int utflen = readShort(data, offset);
        offset += 2;
        char[] chararr = new char[utflen];

        int c, char2, char3;
        int count = 0;
        int chararr_count=0;

        while (count < utflen) {
            c = (int) data[offset + count] & 0xff;      
            if (c > 127) break;
            count++;
            chararr[chararr_count++]=(char)c;
        }

        while (count < utflen) {
            c = (int) data[offset + count] & 0xff;
            switch (c >> 4) {
                case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                    /* 0xxxxxxx*/
                    count++;
                    chararr[chararr_count++]=(char)c;
                    break;
                case 12: case 13:
                    /* 110x xxxx   10xx xxxx*/
                    count += 2;
                    if (count > utflen)
                        throw new UTFDataFormatException(
                            "malformed input: partial character at end");
                    char2 = (int) data[offset + count-1];
                    if ((char2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException(
                            "malformed input around byte " + count); 
                    chararr[chararr_count++]=(char)(((c & 0x1F) << 6) | 
                                                    (char2 & 0x3F));  
                    break;
                case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    count += 3;
                    if (count > utflen)
                        throw new UTFDataFormatException(
                            "malformed input: partial character at end");
                    char2 = (int) data[offset + count-2];
                    char3 = (int) data[offset + count-1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw new UTFDataFormatException(
                            "malformed input around byte " + (count-1));
                    chararr[chararr_count++]=(char)(((c     & 0x0F) << 12) |
                                                    ((char2 & 0x3F) << 6)  |
                                                    ((char3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx,  1111 xxxx */
                    throw new UTFDataFormatException(
                        "malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        stringBuilder.setLength(0);
        stringBuilder.append(chararr, 0, chararr_count);
        return stringBuilder;
    }
}
