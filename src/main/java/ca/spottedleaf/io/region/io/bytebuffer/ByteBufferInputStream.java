package ca.spottedleaf.io.region.io.bytebuffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {

    protected ByteBuffer buffer;

    public ByteBufferInputStream(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    protected ByteBuffer refill(final ByteBuffer current) throws IOException {
        return current;
    }

    @Override
    public int read() throws IOException {
        if (this.buffer.hasRemaining()) {
            return (int)this.buffer.get() & 0xFF;
        }

        this.buffer = this.refill(this.buffer);
        if (!this.buffer.hasRemaining()) {
            return -1;
        }
        return (int)this.buffer.get() & 0xFF;
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

        // only return 0 when len = 0
        if (len == 0) {
            return 0;
        }

        int remaining = this.buffer.remaining();
        if (remaining <= 0) {
            this.buffer = this.refill(this.buffer);
            remaining = this.buffer.remaining();

            if (remaining <= 0) {
                return -1;
            }
        }

        final int toRead = Math.min(remaining, len);
        this.buffer.get(b, off, toRead);

        return toRead;
    }

    public int read(final ByteBuffer dst) throws IOException {
        final int off = dst.position();
        final int len = dst.remaining();

        // assume buffer position/limits are valid

        if (len == 0) {
            return 0;
        }

        int remaining = this.buffer.remaining();
        if (remaining <= 0) {
            this.buffer = this.refill(this.buffer);
            remaining = this.buffer.remaining();

            if (remaining <= 0) {
                return -1;
            }
        }

        final int toRead = Math.min(remaining, len);

        dst.put(off, this.buffer, this.buffer.position(), toRead);

        this.buffer.position(this.buffer.position() + toRead);
        dst.position(off + toRead);

        return toRead;
    }

    @Override
    public long skip(final long n) throws IOException {
        final int remaining = this.buffer.remaining();

        final long toSkip = Math.min(n, (long)remaining);

        if (toSkip > 0) {
            this.buffer.position(this.buffer.position() + (int)toSkip);
        }

        return Math.max(0, toSkip);
    }

    @Override
    public int available() throws IOException {
        return this.buffer.remaining();
    }
}
