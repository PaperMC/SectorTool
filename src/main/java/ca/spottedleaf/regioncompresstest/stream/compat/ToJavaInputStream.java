package ca.spottedleaf.regioncompresstest.stream.compat;

import ca.spottedleaf.regioncompresstest.stream.ByteBufferInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ToJavaInputStream extends InputStream {

    protected final ByteBufferInputStream wrap;
    protected final ByteBuffer buffer;

    public ToJavaInputStream(final ByteBufferInputStream wrap) {
        this(wrap, null);
    }

    public ToJavaInputStream(final ByteBufferInputStream wrap, final ByteBuffer buffer) {
        this.wrap = wrap;
        this.buffer = buffer;
        if (buffer != null) {
            buffer.limit(buffer.capacity());
            buffer.position(0);
        }
    }

    @Override
    public int read() throws IOException {
        return this.wrap.read();
    }

    @Override
    public int read(final byte[] bytes) throws IOException {
        return this.read(bytes, 0, bytes.length);
    }

    @Override
    public int read(final byte[] bytes, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (bytes.length - (off + len))) < 0) {
            // length < 0 || off < 0 || (off + len) < 0
            throw new IndexOutOfBoundsException();
        }

        if (this.buffer == null) {
            return this.wrap.read(ByteBuffer.wrap(bytes), off, len);
        }

        final int toRead = Math.min(len, this.buffer.capacity());
        final int read = this.wrap.read(this.buffer, 0, toRead);

        if (read > 0) {
            this.buffer.get(0, bytes, off, read);
        }

        return read;
    }

    @Override
    public long skip(final long n) throws IOException {
        return this.wrap.skip(n);
    }

    @Override
    public void skipNBytes(final long n) throws IOException {
        this.wrap.skipFully(n);
    }

    @Override
    public int available() throws IOException {
        final long availLong = this.wrap.available();
        if (availLong < (long)Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        if (availLong > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return (int)availLong;
    }

    @Override
    public void close() throws IOException {
        this.wrap.close();
    }
}
