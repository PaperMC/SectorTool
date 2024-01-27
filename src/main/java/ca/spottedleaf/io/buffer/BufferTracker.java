package ca.spottedleaf.io.buffer;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class BufferTracker implements Closeable {

    private static final ByteBuffer[] EMPTY_BYTE_BUFFERS = new ByteBuffer[0];
    private static final byte[][] EMPTY_BYTE_ARRAYS = new byte[0][];

    public final SimpleBufferManager bufferManager;
    private final List<ByteBuffer> directBuffers = new ArrayList<>();
    private final List<byte[]> javaBuffers = new ArrayList<>();

    private boolean released;

    public BufferTracker(final SimpleBufferManager bufferManager) {
        this.bufferManager = bufferManager;
    }

    public BufferTracker scope() {
        return new BufferTracker(this.bufferManager);
    }

    public ByteBuffer acquireDirectBuffer() {
        final ByteBuffer ret = this.bufferManager.acquireDirectBuffer();
        this.directBuffers.add(ret);
        return ret;
    }

    public byte[] acquireJavaBuffer() {
        final byte[] ret = this.bufferManager.acquireJavaBuffer();
        this.javaBuffers.add(ret);
        return ret;
    }

    @Override
    public void close() {
        if (this.released) {
            throw new IllegalStateException("Double-releasing buffers (incorrect class usage?)");
        }
        this.released = true;

        final ByteBuffer[] directBuffers = this.directBuffers.toArray(EMPTY_BYTE_BUFFERS);
        this.directBuffers.clear();
        for (final ByteBuffer buffer : directBuffers) {
            this.bufferManager.returnDirectBuffer(buffer);
        }

        final byte[][] javaBuffers = this.javaBuffers.toArray(EMPTY_BYTE_ARRAYS);
        this.javaBuffers.clear();
        for (final byte[] buffer : javaBuffers) {
            this.bufferManager.returnJavaBuffer(buffer);
        }
    }
}
