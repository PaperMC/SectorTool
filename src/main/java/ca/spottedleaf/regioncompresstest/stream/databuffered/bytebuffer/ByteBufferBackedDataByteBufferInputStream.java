package ca.spottedleaf.regioncompresstest.stream.databuffered.bytebuffer;

import ca.spottedleaf.regioncompresstest.stream.databuffered.AbstractBufferedDataByteBufferInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ByteBufferBackedDataByteBufferInputStream extends AbstractBufferedDataByteBufferInputStream {

    public ByteBufferBackedDataByteBufferInputStream(final ByteBuffer buffer) {
        super(buffer, true);
    }

    @Override
    protected boolean canFillFromSource() {
        return false;
    }

    @Override
    protected int fillFromSource(final int minimum) throws IOException {
        return -1;
    }

    @Override
    protected int readFromSource(final ByteBuffer buffer, final int offset, final int length) throws IOException {
        return -1;
    }

    @Override
    protected long skipSource(final long n) throws IOException {
        return 0L;
    }

    @Override
    protected void skipFullySource(final long n) throws IOException {
        if (n > 0) {
            throw new EOFException();
        }
    }

    @Override
    protected long availableSource() throws IOException {
        return 0L;
    }

    @Override
    protected void closeSource() throws IOException {}
}
