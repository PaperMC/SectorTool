package ca.spottedleaf.sectortool.stream.databuffered;

import ca.spottedleaf.sectortool.stream.ByteBufferInputStream;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class AbstractBufferedDataByteBufferInputStream extends ByteBufferInputStream implements DataInput {

    protected static final int DEFAULT_BUFFER_SIZE = 8192;

    protected static final VarHandle DIRECT_READ_SHORT  = MethodHandles.byteBufferViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
    protected static final VarHandle DIRECT_READ_CHAR   = MethodHandles.byteBufferViewVarHandle(char[].class, ByteOrder.BIG_ENDIAN);
    protected static final VarHandle DIRECT_READ_INT    = MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    protected static final VarHandle DIRECT_READ_LONG   = MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    protected static final VarHandle DIRECT_READ_FLOAT  = MethodHandles.byteBufferViewVarHandle(float[].class, ByteOrder.BIG_ENDIAN);
    protected static final VarHandle DIRECT_READ_DOUBLE = MethodHandles.byteBufferViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);

    protected final ByteBuffer buffer;

    protected AbstractBufferedDataByteBufferInputStream() {
        this(DEFAULT_BUFFER_SIZE);
    }

    protected AbstractBufferedDataByteBufferInputStream(final int bufferSize) {
        if (bufferSize < 32) {
            throw new IllegalArgumentException("Buffer size must be at least 32: " + bufferSize);
        }

        this.buffer = ByteBuffer.allocateDirect(bufferSize);
    }

    protected AbstractBufferedDataByteBufferInputStream(final ByteBuffer buffer) {
        this(buffer, false);
    }

    protected AbstractBufferedDataByteBufferInputStream(final ByteBuffer buffer, final boolean passBuffer) {
        if (buffer == null) {
            throw new NullPointerException("Buffer is null");
        }

        if (!passBuffer) {
            if (!buffer.isDirect()) {
                throw new IllegalArgumentException("Buffer must be direct");
            }

            final int capacity = buffer.capacity();

            if (capacity < 32) {
                throw new IllegalArgumentException("Buffer size must be at least 32: " + capacity);
            }

            buffer.limit(capacity);
            buffer.position(capacity);
        }

        this.buffer = buffer;
    }

    protected boolean canFillFromSource() {
        return true;
    }

    protected abstract int fillFromSource(final int minimum) throws IOException;

    protected abstract int readFromSource(final ByteBuffer buffer, final int offset, final int length) throws IOException;

    protected abstract long skipSource(final long n) throws IOException;

    protected abstract void skipFullySource(final long n) throws IOException;

    protected abstract long availableSource() throws IOException;

    protected abstract void closeSource() throws IOException;

    protected boolean ensure(final int rem, final boolean eof) throws IOException {
        final int pos = this.buffer.position();
        final int limit = this.buffer.limit();

        final int buffered = limit - pos;
        final int required = rem - buffered;
        if (required <= 0) {
            return true;
        }

        if (!this.canFillFromSource()) {
            if (eof) {
                // avoid shifting the content in the buffer unless we know we can possibly fill it
                throw new EOFException();
            } else {
                return false;
            }
        }

        if (buffered > 0 && pos != 0) {
            // need to shift the buffered bytes to the start of the buffer
            this.buffer.put(0, this.buffer, pos, buffered);
        }
        if (buffered < 0) {
            throw new IllegalStateException();
        }

        this.buffer.limit(this.buffer.capacity());
        this.buffer.position(buffered);
        final int actuallyRead = this.fillFromSource(required);

        if (actuallyRead < rem) {
            if (eof) {
                throw new EOFException();
            } else {
                return false;
            }
        }

        return true;
    }

    @Override
    public int read() throws IOException {
        if (!this.ensure(1, false)) {
            return -1;
        }
        return (int)this.buffer.get() & 0xFF;
    }

    @Override
    public int read(final ByteBuffer into, int offset, int length) throws IOException {
        final int intoLimit = into.limit();
        if (((length | offset) | (offset + length) | (intoLimit - (offset + length))) < 0) {
            // length < 0 || off < 0 || (off + len) < 0
            throw new IndexOutOfBoundsException();
        }

        int ret = 0;

        // drain current buffer
        int buffered = Math.min(length, this.buffer.remaining());
        if (buffered != 0) { // note: buffered = 0 if closed
            final int pos = this.buffer.position();
            into.put(offset, this.buffer, pos, buffered);
            this.buffer.position(pos + buffered);

            ret += buffered;
            offset += buffered;
            length -= buffered;
        }

        if (length == 0) {
            return ret;
        }

        final int bufferCapacity = this.buffer.capacity();

        if (length >= bufferCapacity) {
            // bypass the buffer, it is useless
            final int r = this.readFromSource(into, offset, length);
            ret += Math.max(0, r);
            return ret == 0 ? -1 : ret;
        }

        // try to fill the buffer
        this.ensure(1, false);

        // finally, just read out whatever is in the buffer and that's it
        buffered = Math.min(length, this.buffer.remaining());

        if (buffered != 0) {
            final int pos = this.buffer.position();
            into.put(offset, this.buffer, pos, buffered);
            this.buffer.position(pos + buffered);

            ret += buffered;
            offset += buffered;
            length -= buffered;

            return ret;
        }

        return ret == 0 ? -1 : ret;
    }

    @Override
    public int readFully(final ByteBuffer into, int offset, int length) throws IOException {
        if (((length | offset) | (offset + length) | (into.limit() - (offset + length))) < 0) {
            // length < 0 || off < 0 || (off + len) < 0
            throw new IndexOutOfBoundsException();
        }

        int total = 0;
        while (length > 0) {
            final int read = this.read(into, offset, length);
            if (read < 0) {
                throw new EOFException();
            }

            total += read;
            offset += read;
            length -= read;
        }

        return total;
    }

    @Override
    public long skip(long n) throws IOException {
        long ret = Math.min((long)this.buffer.remaining(), n);
        if (ret != 0L) {
            this.buffer.position(this.buffer.position() + (int)ret);
        }

        n -= ret;

        if (n != 0L) {
            ret += this.skipSource(n);
        }

        return ret;
    }

    @Override
    public void skipFully(long n) throws IOException {
        long skipBuffered = Math.min((long)this.buffer.remaining(), n);
        if (skipBuffered != 0L) {
            this.buffer.position(this.buffer.position() + (int)skipBuffered);
        }

        n -= skipBuffered;

        if (n != 0L) {
            this.skipFullySource(n);
        }
    }

    @Override
    public long available() throws IOException {
        return (long)this.buffer.remaining() + this.availableSource();
    }

    @Override
    public void close() throws IOException {
        this.buffer.limit(0);
        this.buffer.position(0);

        this.closeSource();
    }

    // DataInput methods:

    @Override
    public void readFully(final byte[] bytes) throws IOException {
        this.readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(final byte[] bytes, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (bytes.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        while (len != 0) {
            this.ensure(1, true);

            final int position = buffer.position();
            final int toRead = Math.min(len, (buffer.limit() - position));
            buffer.position(position + toRead);

            buffer.get(position, bytes, off, toRead);

            off += toRead;
            len -= toRead;
        }
    }

    @Override
    public int skipBytes(final int n) throws IOException {
        return (int)this.skip((long)n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        this.ensure(1, true);
        return this.buffer.get() != 0;
    }

    @Override
    public byte readByte() throws IOException {
        this.ensure(1, true);
        return this.buffer.get();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        this.ensure(1, true);
        return (int)this.buffer.get() & 0xFF;
    }

    @Override
    public short readShort() throws IOException {
        this.ensure(2, true);
        final int pos = this.buffer.position();
        final short ret = (short)DIRECT_READ_SHORT.get(this.buffer, pos);
        this.buffer.position(pos + 2);
        return ret;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        this.ensure(2, true);
        final int pos = this.buffer.position();
        final char ret = (char)DIRECT_READ_CHAR.get(this.buffer, pos);
        this.buffer.position(pos + 2);
        return (int)ret;
    }

    @Override
    public char readChar() throws IOException {
        this.ensure(2, true);
        final int pos = this.buffer.position();
        final char ret = (char)DIRECT_READ_CHAR.get(this.buffer, pos);
        this.buffer.position(pos + 2);
        return ret;
    }

    @Override
    public int readInt() throws IOException {
        this.ensure(4, true);
        final int pos = this.buffer.position();
        final int ret = (int)DIRECT_READ_INT.get(this.buffer, pos);
        this.buffer.position(pos + 4);
        return ret;
    }

    @Override
    public float readFloat() throws IOException {
        this.ensure(4, true);
        final int pos = this.buffer.position();
        final float ret = (float)DIRECT_READ_FLOAT.get(this.buffer, pos);
        this.buffer.position(pos + 4);
        return ret;
    }

    @Override
    public long readLong() throws IOException {
        this.ensure(8, true);
        final int pos = this.buffer.position();
        final long ret = (long)DIRECT_READ_LONG.get(this.buffer, pos);
        this.buffer.position(pos + 8);
        return ret;
    }

    @Override
    public double readDouble() throws IOException {
        this.ensure(8, true);
        final int pos = this.buffer.position();
        final double ret = (double)DIRECT_READ_DOUBLE.get(this.buffer, pos);
        this.buffer.position(pos + 8);
        return ret;
    }

    @Override
    public String readUTF() throws IOException {
        // TODO optimise by implementing ourselves
        return DataInputStream.readUTF(this);
    }

    @Override
    public String readLine() throws IOException {
        final StringBuilder ret = new StringBuilder();

        for (;;) {
            if (!this.ensure(1, false)) {
                return ret.length() == 0 ? null : ret.toString();
            }

            final int c = (int)this.buffer.get() & 0xFF;

            if (c != '\n' && c != '\r') {
                ret.append((char)c);
                continue;
            }

            if (c == '\n') {
                break;
            }

            // try to read "\n"
            final int pos;
            if (this.ensure(1, false) && ((int)this.buffer.get(pos = this.buffer.position()) & 0xFF) == '\n') {
                this.buffer.position(pos + 1);
            }

            break;
        }

        return ret.toString();
    }

    // specified by us

    public void readFully(final short[] shorts) throws IOException {
        this.readFully(shorts, 0, shorts.length);
    }

    public void readFully(final short[] shorts, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (shorts.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        while (len != 0) {
            this.ensure(2, true);

            final int position = buffer.position();
            final int toRead = Math.min(len, (buffer.limit() - position) >> 1);
            buffer.position(position + (toRead << 1));

            for (int i = buffer.position(), max = i + (toRead << 1); i < max; i += 2) {
                shorts[off++] = (short)DIRECT_READ_SHORT.get(buffer, i);
            }

            len -= toRead;
        }
    }

    public void readFully(final char[] chars) throws IOException {
        this.readFully(chars, 0, chars.length);
    }

    public void readFully(final char[] chars, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (chars.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        while (len != 0) {
            this.ensure(2, true);

            final int position = buffer.position();
            final int toRead = Math.min(len, (buffer.limit() - position) >> 1);
            buffer.position(position + (toRead << 1));

            for (int i = position, max = i + (toRead << 1); i < max; i += 2) {
                chars[off++] = (char)DIRECT_READ_CHAR.get(buffer, i);
            }

            len -= toRead;
        }
    }

    public void readFully(final int[] ints) throws IOException {
        this.readFully(ints, 0, ints.length);
    }

    public void readFully(final int[] ints, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (ints.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        while (len != 0) {
            this.ensure(4, true);

            final int position = buffer.position();
            final int toRead = Math.min(len, (buffer.limit() - position) >> 2);
            buffer.position(position + (toRead << 2));

            for (int i = position, max = i + (toRead << 2); i < max; i += 4) {
                ints[off++] = (int)DIRECT_READ_INT.get(buffer, i);
            }

            len -= toRead;
        }
    }

    public void readFully(final float[] floats) throws IOException {
        this.readFully(floats, 0, floats.length);
    }

    public void readFully(final float[] floats, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (floats.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        while (len != 0) {
            this.ensure(4, true);

            final int position = buffer.position();
            final int toRead = Math.min(len, (buffer.limit() - position) >> 2);
            buffer.position(position + (toRead << 2));

            for (int i = position, max = i + (toRead << 2); i < max; i += 4) {
                floats[off++] = (float)DIRECT_READ_FLOAT.get(buffer, i);
            }

            len -= toRead;
        }
    }

    public void readFully(final long[] longs) throws IOException {
        this.readFully(longs, 0, longs.length);
    }

    public void readFully(final long[] longs, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (longs.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        while (len != 0) {
            this.ensure(8, true);

            final int position = buffer.position();
            final int toRead = Math.min(len, (buffer.limit() - position) >> 3);
            buffer.position(position + (toRead << 3));

            for (int i = position, max = i + (toRead << 3); i < max; i += 8) {
                longs[off++] = (long)DIRECT_READ_LONG.get(buffer, i);
            }

            len -= toRead;
        }
    }

    public void readFully(final double[] doubles) throws IOException {
        this.readFully(doubles, 0, doubles.length);
    }

    public void readFully(final double[] doubles, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (doubles.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        while (len != 0) {
            this.ensure(8, true);

            final int position = buffer.position();
            final int toRead = Math.min(len, (buffer.limit() - position) >> 3);
            buffer.position(position + (toRead << 3));

            for (int i = position, max = i + (toRead << 3); i < max; i += 8) {
                doubles[off++] = (double)DIRECT_READ_DOUBLE.get(buffer, i);
            }

            len -= toRead;
        }
    }
}
