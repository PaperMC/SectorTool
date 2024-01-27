package ca.spottedleaf.io.buffer;

import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;

public final class SimpleBufferManager {

    private final int max;
    private final int size;

    private final ReferenceOpenHashSet<ByteBuffer> allocatedNativeBuffers;
    private final ReferenceOpenHashSet<byte[]> allocatedJavaBuffers;

    private final ArrayDeque<ByteBuffer> nativeBuffers;
    // ByteBuffer.equals is not reference-based...
    private final ReferenceOpenHashSet<ByteBuffer> storedNativeBuffers;
    private final ArrayDeque<byte[]> javaBuffers;

    public SimpleBufferManager(final int maxPer, final int size) {
        this.max = maxPer;
        this.size = size;

        if (maxPer < 0) {
            throw new IllegalArgumentException("'Max per' is negative");
        }

        if (size < 0) {
            throw new IllegalArgumentException("Size is negative");
        }

        final int alloc = Math.min(10, maxPer);

        this.allocatedNativeBuffers = new ReferenceOpenHashSet<>(alloc);
        this.allocatedJavaBuffers = new ReferenceOpenHashSet<>(alloc);

        this.nativeBuffers = new ArrayDeque<>(alloc);
        this.storedNativeBuffers = new ReferenceOpenHashSet<>(alloc);
        this.javaBuffers = new ArrayDeque<>(alloc);
    }

    public BufferTracker tracker() {
        return new BufferTracker(this);
    }

    public ByteBuffer acquireDirectBuffer() {
        ByteBuffer ret;
        synchronized (this) {
            ret = this.nativeBuffers.poll();
            if (ret != null) {
                this.storedNativeBuffers.remove(ret);
            }
        }
        if (ret == null) {
            ret = ByteBuffer.allocateDirect(this.size);
            synchronized (this) {
                this.allocatedNativeBuffers.add(ret);
            }
        }

        ret.order(ByteOrder.BIG_ENDIAN);
        ret.limit(ret.capacity());
        ret.position(0);

        return ret;
    }

    public synchronized void returnDirectBuffer(final ByteBuffer buffer) {
        if (!this.allocatedNativeBuffers.contains(buffer)) {
            throw new IllegalArgumentException("Buffer is not allocated from here");
        }
        if (this.storedNativeBuffers.contains(buffer)) {
            throw new IllegalArgumentException("Buffer is already returned");
        }
        if (this.nativeBuffers.size() < this.max) {
            this.nativeBuffers.addFirst(buffer);
            this.storedNativeBuffers.add(buffer);
        } else {
            this.allocatedNativeBuffers.remove(buffer);
        }
    }

    public byte[] acquireJavaBuffer() {
        byte[] ret;
        synchronized (this) {
            ret = this.javaBuffers.poll();
        }
        if (ret == null) {
            ret = new byte[this.size];
            synchronized (this) {
                this.allocatedJavaBuffers.add(ret);
            }
        }
        return ret;
    }

    public synchronized void returnJavaBuffer(final byte[] buffer) {
        if (!this.allocatedJavaBuffers.contains(buffer)) {
            throw new IllegalArgumentException("Buffer is not allocated from here");
        }
        if (this.javaBuffers.contains(buffer)) {
            throw new IllegalArgumentException("Buffer is already returned");
        }
        if (this.javaBuffers.size() < this.max) {
            this.javaBuffers.addFirst(buffer);
        } else {
            this.allocatedJavaBuffers.remove(buffer);
        }
    }

    public synchronized void clearReturnedBuffers() {
        this.allocatedNativeBuffers.removeAll(this.nativeBuffers);
        this.storedNativeBuffers.removeAll(this.nativeBuffers);
        this.nativeBuffers.clear();

        this.allocatedJavaBuffers.removeAll(this.javaBuffers);
        this.javaBuffers.clear();
    }

    public int getSize() {
        return this.size;
    }
}
