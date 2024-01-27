package ca.spottedleaf.io.buffer;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

public final class ZstdTracker implements Closeable {

    private static final ZstdCompressCtx[] EMPTY_COMPRESSORS = new ZstdCompressCtx[0];
    private static final ZstdDecompressCtx[] EMPTY_DECOMPRSSORS = new ZstdDecompressCtx[0];

    public final ZstdCtxManager zstdCtxManager;
    private final List<ZstdCompressCtx> compressors = new ArrayList<>();
    private final List<ZstdDecompressCtx> decompressors = new ArrayList<>();

    private boolean released;

    public ZstdTracker(final ZstdCtxManager zstdCtxManager) {
        this.zstdCtxManager = zstdCtxManager;
    }

    public ZstdTracker scope() {
        return new ZstdTracker(this.zstdCtxManager);
    }

    public ZstdCompressCtx acquireCompressor() {
        final ZstdCompressCtx ret = this.zstdCtxManager.acquireCompress();
        this.compressors.add(ret);
        return ret;
    }

    public ZstdDecompressCtx acquireDecompressor() {
        final ZstdDecompressCtx ret = this.zstdCtxManager.acquireDecompress();
        this.decompressors.add(ret);
        return ret;
    }

    @Override
    public void close() {
        if (this.released) {
            throw new IllegalStateException("Double-releasing buffers (incorrect class usage?)");
        }
        this.released = true;

        final ZstdCompressCtx[] compressors = this.compressors.toArray(EMPTY_COMPRESSORS);
        this.compressors.clear();
        for (final ZstdCompressCtx compressor : compressors) {
            this.zstdCtxManager.returnCompress(compressor);
        }

        final ZstdDecompressCtx[] decompressors = this.decompressors.toArray(EMPTY_DECOMPRSSORS);
        this.decompressors.clear();
        for (final ZstdDecompressCtx decompressor : decompressors) {
            this.zstdCtxManager.returnDecompress(decompressor);
        }
    }

}
