package ca.spottedleaf.io.buffer;

import java.io.Closeable;

public record BufferChoices(
        /* 16kb sized buffers */
        BufferTracker t16k,
        /* 1mb sized buffers */
        BufferTracker t1m,

        ZstdTracker zstdCtxs
) implements Closeable {

    public static BufferChoices createNew(final int maxPer) {
        return new BufferChoices(
                new SimpleBufferManager(maxPer, 16 * 1024).tracker(),
                new SimpleBufferManager(maxPer, 1 * 1024 * 1024).tracker(),
                new ZstdCtxManager(maxPer).tracker()
        );
    }

    public BufferChoices scope() {
        return new BufferChoices(
                this.t16k.scope(), this.t1m.scope(), this.zstdCtxs.scope()
        );
    }

    @Override
    public void close() {
        this.t16k.close();
        this.t1m.close();
        this.zstdCtxs.close();
    }
}
