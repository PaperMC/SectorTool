package ca.spottedleaf.regioncompresstest.stream.databuffered.wrapped;

import ca.spottedleaf.regioncompresstest.stream.ByteBufferInputStream;
import ca.spottedleaf.regioncompresstest.stream.databuffered.AbstractBufferedDataByteBufferInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

public class WrappedBufferedDataByteBufferInputStream extends AbstractBufferedDataByteBufferInputStream {

    protected final ByteBufferInputStream wrap;

    public WrappedBufferedDataByteBufferInputStream(final ByteBufferInputStream wrap) {
        this(wrap, DEFAULT_BUFFER_SIZE);
    }

    public WrappedBufferedDataByteBufferInputStream(final ByteBufferInputStream wrap, final int bufferSize) {
        super(bufferSize);
        if (wrap == null) {
            throw new NullPointerException("Wrapped stream is null");
        }
        this.wrap = wrap;
    }

    public WrappedBufferedDataByteBufferInputStream(final ByteBufferInputStream wrap, final ByteBuffer buffer) {
        super(buffer);
        if (wrap == null) {
            throw new NullPointerException("Wrapped stream is null");
        }
        this.wrap = wrap;
    }


    @Override
    protected int fillFromSource(final int minimum) throws IOException {
        final ByteBuffer buffer = this.buffer;
        final ByteBufferInputStream wrap = this.wrap;
        int position = buffer.position();
        int limit = buffer.limit();
        int read = 0;
        do {
            final int r = wrap.read(buffer, position, limit - position);
            if (r <= 0) {
                break;
            }
            // advance max
            read += r;
            position += r;
        } while (read < minimum);

        buffer.limit(position);
        buffer.position(0);

        return read == 0 ? -1 : read;
    }

    @Override
    protected int readFromSource(final ByteBuffer buffer, final int offset, final int length) throws IOException {
        return this.wrap.read(buffer, offset, length);
    }

    @Override
    protected long skipSource(final long n) throws IOException {
        return this.wrap.skip(n);
    }

    @Override
    protected void skipFullySource(final long n) throws IOException {
        this.wrap.skipFully(n);
    }

    @Override
    protected long availableSource() throws IOException {
        return this.wrap.available();
    }

    @Override
    protected void closeSource() throws IOException {
        this.wrap.close();
    }
}
