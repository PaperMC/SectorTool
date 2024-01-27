package ca.spottedleaf.io.stream;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ByteBufferOutputStream implements Closeable, Flushable {

    public abstract void write(final int b) throws IOException;

    // updates buffer position
    public final void write(final ByteBuffer src) throws IOException {
        final int off = src.position();
        final int len = src.remaining();

        if (len == 0) {
            return;
        }

        this.write(src, off, len);

        src.position(off + len);
    }

    // does not update buffer position
    public abstract void write(final ByteBuffer src, final int offset, final int length) throws IOException;

    @Override
    public void close() throws IOException {}

    @Override
    public void flush() throws IOException {}
}
