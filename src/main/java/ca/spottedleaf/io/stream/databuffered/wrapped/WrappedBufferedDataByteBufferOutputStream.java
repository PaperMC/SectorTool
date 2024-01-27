package ca.spottedleaf.io.stream.databuffered.wrapped;

import ca.spottedleaf.io.stream.ByteBufferOutputStream;
import ca.spottedleaf.io.stream.databuffered.AbstractBufferedDataByteBufferOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class WrappedBufferedDataByteBufferOutputStream extends AbstractBufferedDataByteBufferOutputStream {

    protected ByteBufferOutputStream wrap;

    public WrappedBufferedDataByteBufferOutputStream(final ByteBufferOutputStream wrap) {
        this(wrap, DEFAULT_BUFFER_SIZE);
    }

    public WrappedBufferedDataByteBufferOutputStream(final ByteBufferOutputStream wrap, final int bufferSize) {
        super(bufferSize);
        if (wrap == null) {
            throw new NullPointerException("Wrapped stream is null");
        }

        this.wrap = wrap;
    }

    public WrappedBufferedDataByteBufferOutputStream(final ByteBufferOutputStream wrap, final ByteBuffer buffer) {
        super(buffer);

        if (wrap == null) {
            throw new NullPointerException("Wrapped stream is null");
        }

        this.wrap = wrap;
    }

    @Override
    protected void writeDestination(final ByteBuffer buffer, final int offset, final int length) throws IOException {
        this.wrap.write(buffer, offset, length);
    }

    @Override
    protected void flushDestination() throws IOException {
        this.wrap.flush();
    }

    @Override
    protected void closeDestination() throws IOException {
        this.wrap.close();
    }
}
