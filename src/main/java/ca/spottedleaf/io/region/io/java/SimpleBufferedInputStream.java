package ca.spottedleaf.io.region.io.java;

import java.io.IOException;
import java.io.InputStream;

public class SimpleBufferedInputStream extends InputStream {

    protected static final int DEFAULT_BUFFER_SIZE = 8192;

    protected InputStream input;
    protected byte[] buffer;
    protected int pos;
    protected int max;

    public SimpleBufferedInputStream(final InputStream input) {
        this(input, DEFAULT_BUFFER_SIZE);
    }

    public SimpleBufferedInputStream(final InputStream input, final int bufferSize) {
        this(input, new byte[bufferSize]);
    }

    public SimpleBufferedInputStream(final InputStream input, final byte[] buffer) {
        if (buffer.length == 0) {
            throw new IllegalArgumentException("Buffer size must be > 0");
        }

        this.input = input;
        this.buffer = buffer;
        this.pos = this.max = 0;
    }

    private void fill() throws IOException {
        if (this.max < 0) {
            // already read EOF
            return;
        }
        // assume pos = buffer.length
        this.max = this.input.read(this.buffer, 0, this.buffer.length);
        this.pos = 0;
    }

    @Override
    public int read() throws IOException {
        if (this.buffer == null) {
            throw new IOException("Closed stream");
        }

        if (this.pos < this.max) {
            return (int)this.buffer[this.pos++] & 0xFF;
        }

        this.fill();

        if (this.pos < this.max) {
            return (int)this.buffer[this.pos++] & 0xFF;
        }

        return -1;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return this.read(b, 0, b.length);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (((len | off) | (off + len) | (b.length - (off + len))) < 0) {
            // length < 0 || off < 0 || (off + len) < 0
            throw new IndexOutOfBoundsException();
        }

        if (this.buffer == null) {
            throw new IOException("Closed stream");
        }

        if (len == 0) {
            return 0;
        }

        if (this.pos >= this.max) {
            if (len >= this.buffer.length) {
                // bypass buffer
                return this.input.read(b, off, len);
            }

            this.fill();
            if (this.pos >= this.max) {
                return -1;
            }
        }

        final int maxRead = Math.min(this.max - this.pos, len);

        System.arraycopy(this.buffer, this.pos, b, off, maxRead);

        this.pos += maxRead;

        return maxRead;
    }

    @Override
    public long skip(final long n) throws IOException {
        final int remaining = this.max - this.pos;

        final long toSkip = Math.min(n, (long)remaining);

        if (toSkip > 0) {
            this.pos += (int)toSkip;
        }

        return Math.max(0, toSkip);
    }

    @Override
    public int available() throws IOException {
        if (this.input == null) {
            throw new IOException("Closed stream");
        }

        final int upper = Math.max(0, this.input.available());
        final int ret = upper + Math.max(0, this.max - this.pos);

        return ret < 0 ? Integer.MAX_VALUE : ret; // ret < 0 when overflow
    }

    @Override
    public void close() throws IOException {
        try {
            this.input.close();
        } finally {
            this.input = null;
            this.buffer = null;
        }
    }
}
