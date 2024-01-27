package ca.spottedleaf.io.region.io.java;

import java.io.IOException;
import java.io.OutputStream;

public class SimpleBufferedOutputStream extends OutputStream {

    protected static final int DEFAULT_BUFFER_SIZE = 8192;

    protected OutputStream output;
    protected byte[] buffer;
    protected int pos;

    public SimpleBufferedOutputStream(final OutputStream output) {
        this(output, DEFAULT_BUFFER_SIZE);
    }

    public SimpleBufferedOutputStream(final OutputStream output, final int bufferSize) {
        this(output, new byte[bufferSize]);
    }

    public SimpleBufferedOutputStream(final OutputStream output, final byte[] buffer) {
        if (buffer.length == 0) {
            throw new IllegalArgumentException("Buffer size must be > 0");
        }

        this.output = output;
        this.buffer = buffer;
        this.pos = 0;
    }

    protected void writeBuffer() throws IOException {
        if (this.pos > 0) {
            this.output.write(this.buffer, 0, this.pos);
            this.pos = 0;
        }
    }

    @Override
    public void write(final int b) throws IOException {
        if (this.buffer == null) {
            throw new IOException("Closed stream");
        }

        if (this.pos < this.buffer.length) {
            this.buffer[this.pos++] = (byte)b;
        } else {
            this.writeBuffer();
            this.buffer[this.pos++] = (byte)b;
        }
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
            final int maxBuffer = Math.min(len, this.buffer.length - this.pos);

            if (maxBuffer == 0) {
                this.writeBuffer();

                if (len >= this.buffer.length) {
                    // bypass buffer
                    this.output.write(b, off, len);
                    return;
                }

                continue;
            }

            System.arraycopy(b, off, this.buffer, this.pos, maxBuffer);
            this.pos += maxBuffer;
            off += maxBuffer;
            len -= maxBuffer;
        }
    }

    @Override
    public void flush() throws IOException {
        this.writeBuffer();
    }

    @Override
    public void close() throws IOException {
        if (this.buffer == null) {
            return;
        }

        try {
            this.flush();
        } finally {
            try {
                this.output.close();
            } finally {
                this.output = null;
                this.buffer = null;
            }
        }
    }
}
