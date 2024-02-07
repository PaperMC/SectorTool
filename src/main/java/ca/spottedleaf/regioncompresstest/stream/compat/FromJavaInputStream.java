package ca.spottedleaf.regioncompresstest.stream.compat;

import ca.spottedleaf.regioncompresstest.stream.ByteBufferInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class FromJavaInputStream extends ByteBufferInputStream {

    protected static final int DEFAULT_BUFFER_SIZE = 8192;

    protected final InputStream wrap;
    protected final byte[] buffer;

    public FromJavaInputStream(final InputStream wrap) {
        this(wrap, DEFAULT_BUFFER_SIZE);
    }

    public FromJavaInputStream(final InputStream wrap, final int bufferSize) {
        this(wrap, new byte[bufferSize]);
    }

    public FromJavaInputStream(final InputStream wrap, final byte[] buffer) {
        this.wrap = wrap;
        this.buffer = buffer;
        if (buffer.length == 0) {
            throw new IllegalArgumentException("Buffer size must be > 0");
        }
    }

    @Override
    public int read() throws IOException {
        return this.wrap.read();
    }

    @Override
    public int read(final ByteBuffer into, final int offset, final int length) throws IOException {
        if (((length | offset) | (offset + length) | (into.limit() - (offset + length))) < 0) {
            // length < 0 || off < 0 || (off + len) < 0
            throw new IndexOutOfBoundsException();
        }

        final InputStream wrap = this.wrap;
        final byte[] buffer = this.buffer;

        if (into.hasArray()) {
            return wrap.read(into.array(), offset, length);
        }

        final int read = Math.min(buffer.length, length);
        final int ret = wrap.read(buffer, 0, read);

        if (ret <= 0) {
            return ret;
        }

        into.put(offset, buffer, 0, ret);

        return ret;
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
    public long skip(final long n) throws IOException {
        return this.wrap.skip(n);
    }

    @Override
    public void skipFully(final long n) throws IOException {
        this.wrap.skipNBytes(n);
    }

    @Override
    public long available() throws IOException {
        return this.wrap.available();
    }

    @Override
    public void close() throws IOException {
        this.wrap.close();
    }
}
