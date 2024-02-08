package ca.spottedleaf.sectortool.stream;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ByteBufferInputStream implements Closeable {

    // reads the next byte, blocking if needed, or rets -1 if EOF
    public abstract int read() throws IOException;

    // updates buffer position
    // reads at least one byte, blocking if needed, or rets -1 if EOF
    public final int read(final ByteBuffer into) throws IOException {
        final int pos = into.position();
        final int lim = into.limit();

        final int read = this.read(into, pos, lim - pos);

        if (read <= 0) {
            return read;
        }

        into.position(pos + read);

        return read;
    }

    // does not update buffer position
    // reads at least one byte, blocking if needed, or rets -1 if EOF
    public abstract int read(final ByteBuffer into, final int offset, final int length) throws IOException;

    // updates buffer position
    // reads at least one byte, blocking if needed, or rets -1 if EOF
    public final int readFully(final ByteBuffer into) throws IOException {
        final int pos = into.position();
        final int lim = into.limit();

        final int read = this.readFully(into, pos, lim - pos);

        into.position(pos + read);

        return read;
    }

    // does not update buffer position
    // reads at least one byte, blocking if needed, or rets -1 if EOF
    public abstract int readFully(final ByteBuffer into, final int offset, final int length) throws IOException;



    public abstract long skip(final long n) throws IOException;

    public abstract void skipFully(final long n) throws IOException;

    public abstract long available() throws IOException;



    @Override
    public void close() throws IOException {}
}
