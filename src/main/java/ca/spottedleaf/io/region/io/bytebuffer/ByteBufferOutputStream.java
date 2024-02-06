package ca.spottedleaf.io.region.io.bytebuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class ByteBufferOutputStream extends OutputStream {

    protected ByteBuffer buffer;

    public ByteBufferOutputStream(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    // always returns a buffer with remaining > 0
    protected abstract ByteBuffer flush(final ByteBuffer current) throws IOException;

    @Override
    public void write(final int b) throws IOException {
        if (this.buffer == null) {
            throw new IOException("Closed stream");
        }

        if (this.buffer.hasRemaining()) {
            this.buffer.put((byte)b);
            return;
        }

        this.buffer = this.flush(this.buffer);
        this.buffer.put((byte)b);
    }

    @Override
    public void write(final byte[] b) throws IOException {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(final byte[] b, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (b.length - (off + len))) < 0) {
            // length < 0 || off < 0 || (off + len) < 0
            throw new IndexOutOfBoundsException();
        }

        if (this.buffer == null) {
            throw new IOException("Closed stream");
        }

        while (len > 0) {
            final int maxWrite = Math.min(this.buffer.remaining(), len);

            if (maxWrite == 0) {
                this.buffer = this.flush(this.buffer);
                continue;
            }

            this.buffer.put(b, off, maxWrite);

            off += maxWrite;
            len -= maxWrite;
        }
    }

    public void write(final ByteBuffer buffer) throws IOException {
        if (this.buffer == null) {
            throw new IOException("Closed stream");
        }

        int off = buffer.position();
        int remaining = buffer.remaining();

        while (remaining > 0) {
            final int maxWrite = Math.min(this.buffer.remaining(), remaining);

            if (maxWrite == 0) {
                this.buffer = this.flush(this.buffer);
                continue;
            }

            final int thisOffset = this.buffer.position();

            this.buffer.put(thisOffset, buffer, off, maxWrite);

            off += maxWrite;
            remaining -= maxWrite;

            // update positions in case flush() throws or needs to be called
            this.buffer.position(thisOffset + maxWrite);
            buffer.position(off);
        }
    }

    @Override
    public void flush() throws IOException {
        if (this.buffer == null) {
            throw new IOException("Closed stream");
        }

        this.buffer = this.flush(this.buffer);
    }

    @Override
    public void close() throws IOException {
        if (this.buffer == null) {
            return;
        }

        try {
            this.flush();
        } finally {
            this.buffer = null;
        }
    }
}
