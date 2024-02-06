package ca.spottedleaf.io.region.io.zstd;

import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferInputStream;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdIOException;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class ZSTDInputStream extends ByteBufferInputStream {

    private ByteBuffer compressedBuffer;
    private ZstdDecompressCtx decompressor;
    private Consumer<ZstdDecompressCtx> closeDecompressor;
    private ByteBufferInputStream wrap;
    private boolean lastDecompressFlushed;
    private boolean done;

    public ZSTDInputStream(final ByteBuffer decompressedBuffer, final ByteBuffer compressedBuffer,
                           final ZstdDecompressCtx decompressor,
                           final Consumer<ZstdDecompressCtx> closeDecompressor,
                           final ByteBufferInputStream wrap) {
        super(decompressedBuffer);

        if (!decompressedBuffer.isDirect() || !compressedBuffer.isDirect()) {
            throw new IllegalArgumentException("Buffers must be direct");
        }

        // set position to max so that we force the first read to go to wrap

        decompressedBuffer.limit(decompressedBuffer.capacity());
        decompressedBuffer.position(decompressedBuffer.capacity());

        compressedBuffer.limit(compressedBuffer.capacity());
        compressedBuffer.position(compressedBuffer.capacity());

        synchronized (this) {
            this.decompressor = decompressor;
            this.closeDecompressor = closeDecompressor;
            this.compressedBuffer = compressedBuffer;
            this.wrap = wrap;
        }
    }

    protected synchronized ByteBuffer refillCompressed(final ByteBuffer current) throws IOException {
        current.limit(current.capacity());
        current.position(0);

        try {
            this.wrap.read(current);
        } finally {
            current.flip();
        }

        return current;
    }

    @Override
    public synchronized int available() throws IOException {
        if (this.decompressor == null) {
            return 0;
        }

        final long ret = (long)super.available() + (long)this.compressedBuffer.remaining() + (long)this.wrap.available();

        if (ret < 0L) {
            return 0;
        } else if (ret > (long)Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return (int)ret;
    }

    @Override
    protected synchronized final ByteBuffer refill(final ByteBuffer current) throws IOException {
        if (this.decompressor == null) {
            throw new EOFException();
        }

        if (this.done) {
            return current;
        }

        ByteBuffer compressedBuffer = this.compressedBuffer;
        final ZstdDecompressCtx decompressor = this.decompressor;

        for (;;) {
            if (!compressedBuffer.hasRemaining()) {
                // try to read more data into source
                this.compressedBuffer = compressedBuffer = this.refillCompressed(compressedBuffer);

                if (!compressedBuffer.hasRemaining()) {
                    // EOF
                    if (!this.lastDecompressFlushed) {
                        throw new ZstdIOException(Zstd.errCorruptionDetected(), "Truncated stream");
                    }
                    return current;
                } else {
                    // more data to decompress, so reset the last flushed
                    this.lastDecompressFlushed = false;
                }
            }

            current.limit(current.capacity());
            current.position(0);

            try {
                this.lastDecompressFlushed = decompressor.decompressDirectByteBufferStream(current, compressedBuffer);
            } finally {
                // if decompressDirectByteBufferStream throws, then current.limit = position = 0
                current.flip();
            }

            if (current.hasRemaining()) {
                return current;
            } else if (this.lastDecompressFlushed) {
                this.done = true;
                return current;
            } // else: need more data
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.decompressor == null) {
            return;
        }

        final ZstdDecompressCtx decompressor = this.decompressor;
        final ByteBufferInputStream wrap = this.wrap;
        final Consumer<ZstdDecompressCtx> closeDecompressor = this.closeDecompressor;
        this.decompressor = null;
        this.compressedBuffer = null;
        this.closeDecompressor = null;
        this.wrap = null;

        try {
            if (closeDecompressor != null) {
                closeDecompressor.accept(decompressor);
            }
        } finally {
            wrap.close();
        }
    }
}
