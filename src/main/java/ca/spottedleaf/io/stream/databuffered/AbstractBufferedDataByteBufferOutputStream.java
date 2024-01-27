package ca.spottedleaf.io.stream.databuffered;

import ca.spottedleaf.io.stream.ByteBufferOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class AbstractBufferedDataByteBufferOutputStream extends ByteBufferOutputStream implements DataOutput {

    protected static final int DEFAULT_BUFFER_SIZE = 8192;

    protected static final VarHandle DIRECT_WRITE_SHORT  = MethodHandles.byteBufferViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
    protected static final VarHandle DIRECT_WRITE_CHAR   = MethodHandles.byteBufferViewVarHandle(char[].class, ByteOrder.BIG_ENDIAN);
    protected static final VarHandle DIRECT_WRITE_INT    = MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    protected static final VarHandle DIRECT_WRITE_LONG   = MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    protected static final VarHandle DIRECT_WRITE_FLOAT  = MethodHandles.byteBufferViewVarHandle(float[].class, ByteOrder.BIG_ENDIAN);
    protected static final VarHandle DIRECT_WRITE_DOUBLE = MethodHandles.byteBufferViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);

    protected final ByteBuffer buffer;
    protected boolean closed;

    public AbstractBufferedDataByteBufferOutputStream() {
        this(DEFAULT_BUFFER_SIZE);
    }

    public AbstractBufferedDataByteBufferOutputStream(final int bufferSize) {
        if (bufferSize < 32) {
            throw new IllegalArgumentException("Buffer size must be at least 32: " + bufferSize);
        }

        this.buffer = ByteBuffer.allocateDirect(bufferSize);
    }

    public AbstractBufferedDataByteBufferOutputStream(final ByteBuffer buffer) {
        if (buffer == null) {
            throw new NullPointerException("Buffer is null");
        }
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Buffer must be direct");
        }

        final int capacity = buffer.capacity();

        if (capacity < 32) {
            throw new IllegalArgumentException("Buffer size must be at least 32: " + capacity);
        }

        buffer.limit(capacity);
        buffer.position(0);

        this.buffer = buffer;
    }

    protected abstract void writeDestination(final ByteBuffer buffer, final int offset, final int length) throws IOException;

    protected abstract void flushDestination() throws IOException;

    protected abstract void closeDestination() throws IOException;

    protected void ensure(final int required) throws IOException {
        if (this.closed) {
            throw new IOException("Closed stream");
        }

        final int remaining = this.buffer.remaining();
        if (remaining >= required) {
            return;
        }

        // flush buffer
        this.flushBuffer();
    }

    public void flushBuffer() throws IOException {
        if (this.closed) {
            throw new IOException("Closed stream");
        }

        final ByteBuffer buffer = this.buffer;
        final int position = buffer.position();

        if (position != 0) {
            this.writeDestination(buffer, 0, position);
            buffer.position(0);
        }
    }

    @Override
    public void write(final int b) throws IOException {
        this.ensure(1);

        this.buffer.put((byte)b);
    }

    @Override
    public void write(final ByteBuffer src, int offset, int length) throws IOException {
        if (((length | offset) | (offset + length) | (src.limit() - (offset + length))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        if (this.closed) {
            throw new IOException("Closed stream");
        }

        if (length == 0) {
            return;
        }

        final ByteBuffer buffer = this.buffer;

        int position = buffer.position();
        int limit = buffer.limit();
        int remaining = limit - position;

        if (position == 0 && length >= limit) {
            // bypass buffer completely
            this.writeDestination(src, offset, length);
            return;
        }

        // store as much as possible into buffer
        if (remaining != 0) {
            final int toPut = Math.min(remaining, length);
            buffer.put(position, src, offset, toPut);

            offset += toPut;
            length -= toPut;
            position += toPut;
            remaining -= toPut;

            buffer.position(position);

            if (length == 0) {
                // nothing left to write
                return;
            }
        }

        // need to flush buffer, as it is full now
        this.flushBuffer();

        if (length >= limit) {
            // bypass buffer
            this.writeDestination(src, offset, length);
            return;
        }

        // store remaining into buffer

        buffer.put(0, src, offset, length);
        buffer.position(length);
    }

    @Override
    public void flush() throws IOException {
        if (this.closed) {
            throw new IOException("Closed stream");
        }

        this.flushBuffer();

        this.flushDestination();
    }

    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }

        try {
            this.flush();
        } finally {
            this.closed = true;
            this.closeDestination();
        }
    }

    @Override
    public void writeBoolean(final boolean v) throws IOException {
        this.writeByte(v ? 1 : 0);
    }

    @Override
    public void writeByte(final int v) throws IOException {
        this.ensure(1);
        this.buffer.put((byte)v);
    }

    @Override
    public void writeShort(final int v) throws IOException {
        this.ensure(2);
        final int position = this.buffer.position();
        this.buffer.position(position + 2);
        DIRECT_WRITE_SHORT.set(this.buffer, position, (short)v);
    }

    @Override
    public void writeChar(final int v) throws IOException {
        this.ensure(2);
        final int position = this.buffer.position();
        this.buffer.position(position + 2);
        DIRECT_WRITE_CHAR.set(this.buffer, position, (char)v);
    }

    @Override
    public void writeInt(final int v) throws IOException {
        this.ensure(4);
        final int position = this.buffer.position();
        this.buffer.position(position + 4);
        DIRECT_WRITE_INT.set(this.buffer, position, (int)v);
    }

    @Override
    public void writeFloat(final float v) throws IOException {
        this.ensure(4);
        final int position = this.buffer.position();
        this.buffer.position(position + 4);
        DIRECT_WRITE_FLOAT.set(this.buffer, position, (float)v);
    }

    @Override
    public void writeLong(final long v) throws IOException {
        this.ensure(8);
        final int position = this.buffer.position();
        this.buffer.position(position + 8);
        DIRECT_WRITE_LONG.set(this.buffer, position, (long)v);
    }

    @Override
    public void writeDouble(final double v) throws IOException {
        this.ensure(8);
        final int position = this.buffer.position();
        this.buffer.position(position + 8);
        DIRECT_WRITE_DOUBLE.set(this.buffer, position, (double)v);
    }

    @Override
    public void writeBytes(final String str) throws IOException {
        int off = 0;
        int len = str.length();
        for (;;) {
            // flush buffers if only we need to (also, checks for EOF)
            this.ensure(1);

            final ByteBuffer buffer = this.buffer;

            final int remaining = buffer.remaining();
            final int maxCopy = Math.min(remaining, len);

            for (int i = off, max = off + maxCopy; i < max; ++i) {
                buffer.put((byte)str.charAt(i));
            }

            off += maxCopy;
            len -= maxCopy;
            if (len == 0) {
                return;
            }
        }
    }

    @Override
    public void writeChars(final String str) throws IOException {
        int off = 0;
        int len = str.length();

        for (;;) {
            if (len == 0) {
                return;
            }

            // flush buffers if only we need to (also, checks for EOF)
            this.ensure(2);

            final ByteBuffer buffer = this.buffer;

            final int position = buffer.position();
            final int maxCopy = Math.min((buffer.limit() - position) >> 1, len);
            buffer.position(position + (maxCopy << 1));

            for (int i = off, max = off + maxCopy; i < max; ++i) {
                DIRECT_WRITE_CHAR.set(buffer, position + (i << 1), (char)str.charAt(i));
            }

            off += maxCopy;
            len -= maxCopy;
        }
    }

    private static final int MAX_UTF_BYTES = 65535;

    @Override
    public void writeUTF(final String str) throws IOException {
        int off = 0;
        int len = str.length();

        // count string length
        int bytes = len; // at least length

        if (bytes > MAX_UTF_BYTES) {
            throw new UTFDataFormatException("Too many bytes (fast-check): " + bytes);
        }

        for (int i = 0; i < len; ++i) {
            final char c = str.charAt(i);
            if (c == 0 || c >= 128) {
                bytes += (c >= 2048) ? 2 : 1;
            }
        }

        if (bytes > MAX_UTF_BYTES) {
            throw new UTFDataFormatException("Too many bytes (slow-check): " + bytes);
        }

        this.writeShort(bytes);

        // attempt ASCII-only write
        if (bytes == len) {
            this.writeBytes(str);
            return;
        }

        // no ASCII only, so we need to use the modified encoding
        for (;;) {
            this.ensure(3);

            final ByteBuffer buffer = this.buffer;
            final int position = buffer.position();

            // assume worst-case that each character could take the full 3 bytes
            final int remaining = (buffer.limit() - position) / 3;
            final int maxCopy = Math.min(remaining, len);

            int bufferIdx = position;
            for (int i = off, max = off + maxCopy; i < max; ++i) {
                final char c = str.charAt(i);
                if (c != 0 && c < 128) {
                    buffer.put(bufferIdx, (byte)c);

                    bufferIdx += 1;
                } else if (c < 2048) {
                    buffer.put(bufferIdx, (byte)(0xC0 | ((c >> 6) & 0x1F)));
                    buffer.put(bufferIdx + 1, (byte)(0x80 | (c & 0x3F)));

                    bufferIdx += 2;
                } else {
                    buffer.put(bufferIdx, (byte)(0xE0 | ((c >> 12) & 0x0F)));
                    buffer.put(bufferIdx + 1, (byte)(0x80 | ((c >> 6) & 0x3F)));
                    buffer.put(bufferIdx + 2, (byte)(0x80 | (c & 0x3F)));

                    bufferIdx += 3;
                }
            }
            buffer.position(bufferIdx);

            off += maxCopy;
            len -= maxCopy;
            if (len == 0) {
                return;
            }
        }
    }

    @Override
    public void write(final byte[] bytes) throws IOException {
        this.write(bytes, 0, bytes.length);
    }

    @Override
    public void write(final byte[] bytes, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (bytes.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        for (;;) {
            if (len == 0) {
                return;
            }

            // flush buffers if only we need to (also, checks for EOF)
            this.ensure(1);

            final int position = buffer.position();
            final int maxCopy = Math.min((buffer.limit() - position), len);
            buffer.position(position + maxCopy);

            buffer.put(position, bytes, off, maxCopy);

            off += maxCopy;
            len -= maxCopy;
        }
    }

    public void write(final short[] shorts) throws IOException {
        this.write(shorts, 0, shorts.length);
    }

    public void write(final short[] shorts, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (shorts.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        for (;;) {
            if (len == 0) {
                return;
            }

            // flush buffers if only we need to (also, checks for EOF)
            this.ensure(2);

            final int position = buffer.position();
            final int maxCopy = Math.min((buffer.limit() - position) >> 1, len);
            buffer.position(position + (maxCopy << 1));

            for (int i = off, max = off + maxCopy, k = 0; i < max; ++i, ++k) {
                DIRECT_WRITE_SHORT.set(buffer, position + (k << 1), shorts[i]);
            }

            off += maxCopy;
            len -= maxCopy;
        }
    }

    public void write(final char[] chars) throws IOException {
        this.write(chars, 0, chars.length);
    }

    public void write(final char[] chars, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (chars.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        for (;;) {
            if (len == 0) {
                return;
            }

            // flush buffers if only we need to (also, checks for EOF)
            this.ensure(2);

            final int position = buffer.position();
            final int maxCopy = Math.min((buffer.limit() - position) >> 1, len);
            buffer.position(position + (maxCopy << 1));

            for (int i = off, max = off + maxCopy, k = 0; i < max; ++i, ++k) {
                DIRECT_WRITE_CHAR.set(buffer, position + (k << 1), chars[i]);
            }

            off += maxCopy;
            len -= maxCopy;
        }
    }

    public void write(final int[] ints) throws IOException {
        this.write(ints, 0, ints.length);
    }

    public void write(final int[] ints, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (ints.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        for (;;) {
            if (len == 0) {
                return;
            }

            // flush buffers if only we need to (also, checks for EOF)
            this.ensure(4);

            final int position = buffer.position();
            final int maxCopy = Math.min((buffer.limit() - position) >> 2, len);
            buffer.position(position + (maxCopy << 2));

            for (int i = off, max = off + maxCopy, k = 0; i < max; ++i, ++k) {
                DIRECT_WRITE_INT.set(buffer, position + (k << 2), ints[i]);
            }

            off += maxCopy;
            len -= maxCopy;
        }
    }

    public void write(final float[] floats) throws IOException {
        this.write(floats, 0, floats.length);
    }

    public void write(final float[] floats, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (floats.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        for (;;) {
            if (len == 0) {
                return;
            }

            // flush buffers if only we need to (also, checks for EOF)
            this.ensure(4);

            final int position = buffer.position();
            final int maxCopy = Math.min((buffer.limit() - position) >> 2, len);
            buffer.position(position + (maxCopy << 2));

            for (int i = off, max = off + maxCopy, k = 0; i < max; ++i, ++k) {
                DIRECT_WRITE_FLOAT.set(buffer, position + (k << 2), floats[i]);
            }

            off += maxCopy;
            len -= maxCopy;
        }
    }

    public void write(final long[] longs) throws IOException {
        this.write(longs, 0, longs.length);
    }

    public void write(final long[] longs, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (longs.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        for (;;) {
            if (len == 0) {
                return;
            }

            // flush buffers if only we need to (also, checks for EOF)
            this.ensure(8);

            final int position = buffer.position();
            final int maxCopy = Math.min((buffer.limit() - position) >> 3, len);
            buffer.position(position + (maxCopy << 3));

            for (int i = off, max = off + maxCopy, k = 0; i < max; ++i, ++k) {
                DIRECT_WRITE_LONG.set(buffer, position + (k << 3), longs[i]);
            }

            off += maxCopy;
            len -= maxCopy;
        }
    }

    public void write(final double[] doubles) throws IOException {
        this.write(doubles, 0, doubles.length);
    }

    public void write(final double[] doubles, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (doubles.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final ByteBuffer buffer = this.buffer;

        for (;;) {
            if (len == 0) {
                return;
            }

            // flush buffers if only we need to (also, checks for EOF)
            this.ensure(8);

            final int position = buffer.position();
            final int maxCopy = Math.min((buffer.limit() - position) >> 3, len);
            buffer.position(position + (maxCopy << 3));

            for (int i = off, max = off + maxCopy, k = 0; i < max; ++i, ++k) {
                DIRECT_WRITE_DOUBLE.set(buffer, position + (k << 3), doubles[i]);
            }

            off += maxCopy;
            len -= maxCopy;
        }
    }
}
