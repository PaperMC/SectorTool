package ca.spottedleaf.io.buffer;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;
import java.util.ArrayDeque;
import java.util.function.Supplier;

public final class ZstdCtxManager {

    private final int max;

    private final ReferenceOpenHashSet<ZstdCompressCtx> allocatedCompress;
    private final ReferenceOpenHashSet<ZstdDecompressCtx> allocatedDecompress;

    private final ArrayDeque<ZstdCompressCtx> compressors;
    private final ArrayDeque<ZstdDecompressCtx> decompressors;

    public ZstdCtxManager(final int maxPer) {
        this.max = maxPer;

        if (maxPer < 0) {
            throw new IllegalArgumentException("'Max per' is negative");
        }

        final int alloc = Math.min(10, maxPer);

        this.allocatedCompress = new ReferenceOpenHashSet<>(alloc);
        this.allocatedDecompress = new ReferenceOpenHashSet<>(alloc);

        this.compressors = new ArrayDeque<>(alloc);
        this.decompressors = new ArrayDeque<>(alloc);
    }

    public ZstdTracker tracker() {
        return new ZstdTracker(this);
    }

    public ZstdCompressCtx acquireCompress() {
        ZstdCompressCtx ret;
        synchronized (this) {
            ret = this.compressors.poll();
        }
        if (ret == null) {
            ret = new ZstdCompressCtx();
            synchronized (this) {
                this.allocatedCompress.add(ret);
            }
        }

        ret.reset();

        return ret;
    }

    public synchronized void returnCompress(final ZstdCompressCtx compressor) {
        if (!this.allocatedCompress.contains(compressor)) {
            throw new IllegalArgumentException("Compressor is not allocated from here");
        }
        if (this.compressors.contains(compressor)) {
            throw new IllegalArgumentException("Compressor is already returned");
        }
        if (this.compressors.size() < this.max) {
            this.compressors.addFirst(compressor);
        } else {
            this.allocatedCompress.remove(compressor);
        }
    }

    public ZstdDecompressCtx acquireDecompress() {
        ZstdDecompressCtx ret;
        synchronized (this) {
            ret = this.decompressors.poll();
        }
        if (ret == null) {
            ret = new ZstdDecompressCtx();
            synchronized (this) {
                this.allocatedDecompress.add(ret);
            }
        }

        ret.reset();

        return ret;
    }

    public synchronized void returnDecompress(final ZstdDecompressCtx decompressor) {
        if (!this.allocatedDecompress.contains(decompressor)) {
            throw new IllegalArgumentException("Decompressor is not allocated from here");
        }
        if (this.decompressors.contains(decompressor)) {
            throw new IllegalArgumentException("Decompressor is already returned");
        }
        if (this.decompressors.size() < this.max) {
            this.decompressors.addFirst(decompressor);
        } else {
            this.allocatedDecompress.remove(decompressor);
        }
    }

    public synchronized void clearReturnedBuffers() {
        this.allocatedCompress.removeAll(this.compressors);
        ZstdCompressCtx compress;
        while ((compress = this.compressors.poll()) != null) {
            compress.close();
        }

        this.allocatedDecompress.removeAll(this.decompressors);
        ZstdDecompressCtx decompress;
        while ((decompress = this.decompressors.poll()) != null) {
            decompress.close();
        }
    }
}
