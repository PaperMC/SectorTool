package ca.spottedleaf.regioncompresstest.stream.databuffered.zstd;

import ca.spottedleaf.regioncompresstest.stream.databuffered.AbstractBufferedDataByteBufferOutputStream;
import com.github.luben.zstd.EndDirective;
import com.github.luben.zstd.ZstdCompressCtx;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public abstract class AbstractZSTDBufferedDataByteBufferOutputStream extends AbstractBufferedDataByteBufferOutputStream {

    protected static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0);

    protected ZstdCompressCtx compressor;
    protected ByteBuffer compressedBuffer;
    protected boolean lastCompressionEnded = false;

    protected final Consumer<ZstdCompressCtx> closeCompressor;

    protected AbstractZSTDBufferedDataByteBufferOutputStream() {
        this(
                ByteBuffer.allocateDirect(AbstractBufferedDataByteBufferOutputStream.DEFAULT_BUFFER_SIZE)
        );
    }

    protected AbstractZSTDBufferedDataByteBufferOutputStream(final ByteBuffer decompressedBuffer) {
        this(decompressedBuffer, new ZstdCompressCtx());
    }

    protected AbstractZSTDBufferedDataByteBufferOutputStream(final ByteBuffer decompressedBuffer,
                                                             final ZstdCompressCtx compressor) {
        this(decompressedBuffer, compressor, ZstdCompressCtx::close);
    }

    protected AbstractZSTDBufferedDataByteBufferOutputStream(final ByteBuffer decompressedBuffer,
                                                             final ZstdCompressCtx compressor,
                                                             final Consumer<ZstdCompressCtx> closeCompressor) {
        super(decompressedBuffer);

        this.closeCompressor = closeCompressor;

        if (decompressedBuffer == null) {
            throw new NullPointerException("Decompressed buffer is null");
        }
        if (!decompressedBuffer.isDirect()) {
            throw new IllegalArgumentException("Decompressed buffer must be direct");
        }

        // do not rely on proper publishing
        synchronized (this) {
            this.compressor = compressor;
        }
    }

    /* https://facebook.github.io/zstd/zstd_manual.html "Streaming compression - HowTo" */

    // note: unsafe to modify compressor if compressing has already begun
    public synchronized ZstdCompressCtx getCompressor() {
        return this.compressor;
    }

    protected abstract ByteBuffer writeZSTDSource(final ByteBuffer buffer) throws IOException;

    protected final ByteBuffer flushCompressedIfNeeded() throws IOException {
        final ByteBuffer compressed = this.compressedBuffer;
        if (compressed == null || compressed.hasRemaining()) {
            return compressed;
        }

        compressed.flip();
        return this.compressedBuffer = this.writeZSTDSource(compressed);
    }

    protected final ByteBuffer flushCompressed() throws IOException {
        final ByteBuffer compressed = this.compressedBuffer;
        if (compressed == null || compressed.position() == 0) {
            return compressed;
        }

        compressed.flip();
        return this.compressedBuffer = this.writeZSTDSource(compressed);
    }

    @Override
    protected synchronized final void writeDestination(final ByteBuffer buffer, final int offset, final int length) throws IOException {
        if (((length | offset) | (offset + length) | (buffer.limit() - (offset + length))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        if (length == 0) {
            return;
        }

        final int prevPos = buffer.position();
        final int prevLimit = buffer.limit();

        // new data
        this.lastCompressionEnded = false;

        buffer.limit(offset + length);
        buffer.position(offset);
        try {
            while (buffer.hasRemaining()) {
                final ByteBuffer compressed = this.flushCompressedIfNeeded();

                this.compressor.compressDirectByteBufferStream(compressed, buffer, EndDirective.CONTINUE);
            }
        } finally {
            buffer.limit(prevLimit);
            buffer.position(prevPos);
        }
    }

    protected abstract void flushZSTDDestination() throws IOException;

    @Override
    protected synchronized final void flushDestination() throws IOException {
        while (!this.lastCompressionEnded) {
            this.lastCompressionEnded = this.compressor.compressDirectByteBufferStream(this.flushCompressedIfNeeded(), EMPTY_BUFFER, EndDirective.END);
        }
        this.flushCompressed();
        this.flushZSTDDestination();
    }

    protected abstract void closeZSTDDestination() throws IOException;

    @Override
    protected synchronized final void closeDestination() throws IOException {
        try {
            this.flushDestination();
        } finally {
            final ZstdCompressCtx compressor = this.compressor;
            try {
                this.compressor = null;
                this.compressedBuffer = null;

                this.closeZSTDDestination();
            } finally {
                if (this.closeCompressor != null) {
                    this.closeCompressor.accept(compressor);
                }
            }
        }
    }
}
