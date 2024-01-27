package ca.spottedleaf.io.stream.compat;

import ca.spottedleaf.io.stream.ByteBufferOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ToJavaOutputStream extends OutputStream {

    protected final ByteBufferOutputStream wrap;
    protected final ByteBuffer buffer;

    public ToJavaOutputStream(final ByteBufferOutputStream wrap) {
        this(wrap, null);
    }
    public ToJavaOutputStream(final ByteBufferOutputStream wrap, final ByteBuffer buffer) {
        this.wrap = wrap;
        this.buffer = buffer;
        if (buffer != null) {
            buffer.limit(buffer.capacity());
            buffer.position(0);
        }
    }

    @Override
    public void write(final int value) throws IOException {
        this.wrap.write(value);
    }

    @Override
    public void write(final byte[] bytes) throws IOException {
        this.write(bytes, 0, bytes.length);
    }

    @Override
    public void write(final byte[] bytes, int off, int len) throws IOException {
        if (((len | off) | (off + len) | (bytes.length - (off + len))) < 0) {
            // length < 0 || off < 0 || (off + len) < 0
            throw new IndexOutOfBoundsException();
        }
        if (this.buffer == null) {
            this.wrap.write(ByteBuffer.wrap(bytes), off, len);
            return;
        }

        while (len > 0) {
            final int maxWrite = Math.min(len, this.buffer.capacity());
            this.buffer.put(0, bytes, off, maxWrite);

            this.wrap.write(this.buffer, 0, maxWrite);

            off += maxWrite;
            len -= maxWrite;
        }
    }

    @Override
    public void flush() throws IOException {
        this.wrap.flush();
    }

    @Override
    public void close() throws IOException {
        this.wrap.close();
    }
}
