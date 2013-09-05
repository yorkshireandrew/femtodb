package femtodb;

import java.io.UTFDataFormatException;

public class BuffRead {

	static byte[] readBuffer2 = new byte[8];
	/** read a single byte */
    static final int read(final byte[] buff, final int index)
    {
    	return buff[index] & 0xFF;
    }
    
    static final void readFully(final byte[] buff, final int index, final byte[] dest, final int destPos, final int len)
    {
    	
    	System.arraycopy(buff, index, dest, destPos, len);
    }
    
    static final byte[] readByteArray(final byte[] buff, int index)
    {
    	int len = readInt(buff,index);
    	index += 4;
    	byte[] retval = new byte[len];
    	for(int x = 0; x < len ; x++)
    	{
    		retval[x] = (byte)read(buff,index++);
    	}
    	return retval;
    }

    static final boolean readBoolean(final byte[] buff, final int index)
    {
    	byte ch = buff[index];
    	return (ch != 0);
    }
    
    static final short readShort(final byte[] buff, int index)
    {
        int ch1 = buff[index++] & 0xFF;
        int ch2 = buff[index] & 0xFF;
        return (short)((ch1 << 8) + (ch2 << 0));
    }

    static final char readChar(final byte[] buff, int index)
    {
        int ch1 = buff[index++] & 0xFF;
        int ch2 = buff[index] & 0xFF;
        return (char)((ch1 << 8) + (ch2 << 0));
    }

    static final int readInt(final byte[] buff, int index)
    {
        int ch1 = buff[index++] & 0xFF;
        int ch2 = buff[index++] & 0xFF;
        int ch3 = buff[index++] & 0xFF;
        int ch4 = buff[index] & 0xFF;
        return ((ch1 << 24) | (ch2 << 16) | (ch3 << 8) | (ch4 << 0));
    }
   
    synchronized
    static final long readLong(final byte[] buff, int index)
    {
    	byte[] readBuffer2L = readBuffer2;
        readFully(buff, index, readBuffer2L, 0, 8);
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

    static final float readFloat(final byte[] buff, final int index){
    	return Float.intBitsToFloat(readInt(buff, index));
    }


    static final double readDouble(final byte[] buff, final int index){
    	return Double.longBitsToDouble(readLong(buff, index));
    }
    
    static final char[] readChars(final byte[] buff, int index)
    {
    	int len = readShort(buff,index);
    	index += 2;
    	char[] retval = new char[len];
    	for(int x = 0; x < len; x++)
    	{
    		retval[x] = readChar(buff,index);
    	}
    	return retval;
    }

    static final String readString(final byte[] buff, int index) throws UTFDataFormatException {
        int utflen = readShort(buff, index);
        index += 2;
        byte[] bytearr = new byte[utflen];
        char[] chararr = new char[utflen];

        int c, char2, char3;
        int count = 0;
        int chararr_count=0;

        readFully(buff, index, bytearr, 0, utflen);

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;      
            if (c > 127) break;
            count++;
            chararr[chararr_count++]=(char)c;
        }

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
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
                    char2 = (int) bytearr[count-1];
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
                    char2 = (int) bytearr[count-2];
                    char3 = (int) bytearr[count-1];
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
}
