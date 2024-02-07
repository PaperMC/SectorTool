package ca.spottedleaf.regioncompresstest.stream.compat;

import ca.spottedleaf.regioncompresstest.stream.ByteBufferOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class FromJavaOutputStream extends ByteBufferOutputStream {

    protected static final int DEFAULT_BUFFER_SIZE = 8192;

    protected final OutputStream wrap;
    protected final byte[] buffer;

    public FromJavaOutputStream(final OutputStream wrap) {
        this(wrap, DEFAULT_BUFFER_SIZE);
    }

    public FromJavaOutputStream(final OutputStream wrap, final int bufferSize) {
        this(wrap, new byte[bufferSize]);
    }

    public FromJavaOutputStream(final OutputStream wrap, final byte[] buffer) {
        this.wrap = wrap;
        this.buffer = buffer;
        if (buffer.length == 0) {
            throw new IllegalArgumentException("Buffer size must be > 0");
        }
    }

    @Override
    public void write(final int b) throws IOException {
        this.wrap.write(b);
    }

    @Override
    public void write(final ByteBuffer src, int offset, int length) throws IOException {
        if (((length | offset) | (offset + length) | (src.limit() - (offset + length))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final OutputStream wrap = this.wrap;
        final byte[] buffer = this.buffer;

        if (!src.hasArray()) {
            while (length > 0) {
                final int copied = Math.min(buffer.length, length);
                src.get(offset, buffer, 0, copied);

                wrap.write(buffer, 0, copied);

                offset += copied;
                length -= copied;
            }
        } else {
            wrap.write(src.array(), offset, length);
        }
    }

    @Override
    public void close() throws IOException {
        this.wrap.close();
    }

    @Override
    public void flush() throws IOException {
        this.wrap.flush();
    }
}
