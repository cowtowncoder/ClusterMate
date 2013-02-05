package com.fasterxml.clustermate.jaxrs.testutil;

import java.io.ByteArrayOutputStream;

/**
 * Implementation of the highest level partitioning of key spaces; typically
 * mapped to different using applications/clients or such.
 */
public final class CustomerId
{
    // Fundamental limit we enforce is that we allow up to 9 digits (not including
    // ignorable leading zeroes)
    private final static int MAX_DIGITS = 9;

    // protected to give unit test access
    protected final static int MAX_NUMERIC = 999999;

    private final static int MAX_NON_MNEMONIC = 0x40FFFFFF;
    
    /**
     * We use this marker value to denote internal value of 0, which is typically
     * used as sort of "null Object".
     */
    public final static CustomerId NA = new CustomerId(0, "0");
    
    protected final int _value;

    protected volatile String _asString;

    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */
    
    protected CustomerId(int internalValue, String stringRepr) {
        _value = internalValue;
        _asString = stringRepr;
    }
    
    public static CustomerId valueOf(int numeric)
    {
        if (numeric <= 0) {
            if (numeric == 0) {
                return NA;
            }
            throw new IllegalArgumentException("Invalid Partition Id ("+numeric+"): can not be negative number");
        }
        if (numeric <= MAX_NUMERIC) {
            return new CustomerId(numeric, null);
        }
        // Intermediate range...
        if (numeric <= MAX_NON_MNEMONIC) {
            _throwTooBig(String.valueOf(numeric));
        }
        // And then "numeric version of mnenomic" case
        int ch = (numeric >>> 24);
        if (ch < 'A' || ch > 'Z') {
            throw new IllegalArgumentException("Invalid Partition Id ("+numeric+"): first character code (0x"
                    +Integer.toHexString(ch)+" is not an upper-case letter");
        }
        ch = (ch << 8) | _validateChar(numeric >> 16 , 2, numeric);
        ch = (ch << 8) | _validateChar(numeric >> 8, 3, numeric);
        ch = (ch << 8) | _validateChar(numeric, 4, numeric);
        return new CustomerId(ch, null);
    }

    public static CustomerId valueOf(String mnemonic)
    {
        int len = (mnemonic == null) ? 0 : mnemonic.length();
        if (len == 0) {
            return NA;
        }
        // First things first: numeric or mnenomic? First character determines
        int ch = mnemonic.charAt(0);
        if (ch <= 'Z' && ch >= 'A') {
            return _menomicValueOf(mnemonic, ch);
        }
        if (ch <= '9' && ch >= '0') {
            return _numericValueOf(mnemonic, ch, len);
        }
        throw new IllegalArgumentException("Invalid partition '"+mnemonic+"': does not start with upper-case letter or digit");
    }

    public static CustomerId from(byte[] buffer, int offset)
    {
        int rawId = ((buffer[offset++]) << 24)
                | ((buffer[offset++] & 0xFF) << 16)
                | ((buffer[offset++] & 0xFF) << 8)
                | (buffer[offset++] & 0xFF)
                ;
        return valueOf(rawId);
    }
    
    protected static CustomerId _menomicValueOf(String mnemonic, int ch)
    {
        // and obey the other constraints: start with upper case ASCII letter (A-Z)
        // and then have 3 upper-case ASCII letters, numbers and/or underscores
        ch = (ch << 8) | _validateChar(mnemonic, 1);
        ch = (ch << 8) | _validateChar(mnemonic, 2);
        ch = (ch << 8) | _validateChar(mnemonic, 3);
        return new CustomerId(ch, mnemonic);
    }

    protected static CustomerId _numericValueOf(String mnemonic, int ch, final int len)
    {
        ch -= '0';
        // first: trim leading zeroes, if any; also handles "all zeroes" case
        int i = 1;
        if (ch == 0) {
            while (true) {
                if (i == len) { // all zeroes
                    return NA;
                }
                if (mnemonic.charAt(i) != '0') {
                    break;
                }
                ++i;
            }
        }
        
        // Then ensure it's all numbers; enforce max length as well
        int nonZeroDigits = 0;
        for (; i < len; ++i) {
            int c = mnemonic.charAt(i);
            if (c > '9' || c < '0') {
                throw new IllegalArgumentException("Invalid numeric Partition Id '"+mnemonic+"': contains non-digit characters");
            }
            if (++nonZeroDigits > MAX_DIGITS) {
                _throwTooBig(mnemonic);
            }
            ch = (ch * 10) + (c - '0');
        }
        // may still have too big magnitude
        if (ch > MAX_NUMERIC) {
            _throwTooBig(mnemonic);
        }
        // let's not pass String as it may be non-canonical (leading zeroes)
        return new CustomerId(ch, null);
    }

    private static void _throwTooBig(String idStr) {
        throw new IllegalArgumentException("Invalid Partition Id ("+idStr+"): numeric ids can not exceed "+MAX_NUMERIC);
    }
    
    private static int _validateChar(String mnemonic, int index)
    {
        char c = mnemonic.charAt(index);
        if (_isValidChar(c)) {
            return c;
        }
        throw new IllegalArgumentException("Invalid Partition Id '"+mnemonic+"': character #"+index
                +" invalid; has to be an upper-case letter, number or underscore");
    }

    private static int _validateChar(int ch, int index, int full)
    {
        ch = ch & 0xFF;
        if (_isValidChar(ch)) {
            return ch;
        }
        throw new IllegalArgumentException("Invalid byte #"+index+"/4 of alleged Partition Id 0x"+Integer.toHexString(full)
                +": has to be an upper-case letter, number or underscore");
    }
    
    private final static boolean _isValidChar(int ch)
    {
        return ((ch <= 'Z' && ch >= 'A')
                || (ch <= '9' && ch >= '0')
                || (ch == '_'));
    }
    
    /*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */

    /**
     * 
     * @return True if value is "mnemonic", that is, expressed as 4-character
     *    String; false if not (and is expressed as number)
     */
    public boolean isMnemonic() {
        return (_value > MAX_NON_MNEMONIC);
    }
    
    /**
     * Accessor for getting the internal 32-bit int representation, used
     * for efficient storage.
     */
    public int asInt() {
        return _value;
    }

    public StringBuilder append(StringBuilder sb) {
        sb.append(toString());
        return sb;
    }

    public ByteArrayOutputStream append(ByteArrayOutputStream bytes) {
        bytes.write(_value >> 24);
        bytes.write(_value >> 16);
        bytes.write(_value >> 8);
        bytes.write(_value);
        return bytes;
    }

    public int append(byte[] buffer, int offset) {
        buffer[offset++] = (byte)(_value >> 24);
        buffer[offset++] = (byte)(_value >> 16);
        buffer[offset++] = (byte)(_value >> 8);
        buffer[offset++] = (byte) _value;
        return offset;
    }
    
    /*
    /**********************************************************************
    /* Overridden standard methods
    /**********************************************************************
     */

    @Override
    public String toString() {
        String str = _asString;
        if (str == null) {
            str = _toString(_value);
            _asString = str;
        }
        return str;
    }

    public static String toString(int raw) {
        return _toString(raw);
    }
    
    @Override public int hashCode() { return _value; }
    
    @Override
    public boolean equals(Object other)
    {
        if (other == this) return true;
        if (other == null) return false;
        if (other.getClass() != getClass()) return false;
        return ((CustomerId) other)._value == _value;
    }

    private final static String _toString(int value)
    {
        if (value <= MAX_NON_MNEMONIC) {
            return String.valueOf(value);
        }
        StringBuilder sb = new StringBuilder(4);
        sb.append((char) (value >>> 24));
        sb.append((char) ((value >> 16) & 0xFF));
        sb.append((char) ((value >> 8) & 0xFF));
        sb.append((char) (value & 0xFF));
        return sb.toString();
    }

}
