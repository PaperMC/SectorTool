package ca.spottedleaf.io.stream.databuffered.zstd;

import ca.spottedleaf.io.stream.databuffered.AbstractBufferedDataByteBufferInputStream;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdIOException;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public abstract class AbstractZSTDBufferedDataByteBufferInputStream extends AbstractBufferedDataByteBufferInputStream {

    protected ZstdDecompressCtx decompressor;
    protected ByteBuffer compressedBuffer;
    protected boolean lastDecompressFlushed = false;
    protected boolean closed;

    protected final Consumer<ZstdDecompressCtx> closeDecompressor;

    protected AbstractZSTDBufferedDataByteBufferInputStream() {
        this(
                ByteBuffer.allocateDirect(AbstractBufferedDataByteBufferInputStream.DEFAULT_BUFFER_SIZE)
        );
    }

    protected AbstractZSTDBufferedDataByteBufferInputStream(final ByteBuffer decompressedBuffer) {
        this(decompressedBuffer, new ZstdDecompressCtx());
    }

    protected AbstractZSTDBufferedDataByteBufferInputStream(final ByteBuffer decompressedBuffer,
                                                            final ZstdDecompressCtx decompressor) {
        this(decompressedBuffer, decompressor, ZstdDecompressCtx::close);
    }

    protected AbstractZSTDBufferedDataByteBufferInputStream(final ByteBuffer decompressedBuffer,
                                                            final ZstdDecompressCtx decompressor,
                                                            final Consumer<ZstdDecompressCtx> closeDecompressor) {
        super(decompressedBuffer);

        this.closeDecompressor = closeDecompressor;

        if (decompressedBuffer == null) {
            throw new NullPointerException("Decompressed buffer is null");
        }
        if (!decompressedBuffer.isDirect()) {
            throw new IllegalArgumentException("Decompressed buffer must be direct");
        }

        // do not rely on proper publishing
        synchronized (this) {
            this.decompressor = decompressor;
        }
    }

    protected final void checkClosed() throws IOException {
        if (this.closed) {
            throw new IOException("Closed stream");
        }
    }

    protected final boolean fill() throws IOException {
        return this.ensure(1, false);
    }

    // return oldBuffer if no new data is present
    protected abstract ByteBuffer readFromZSTDSource(final ByteBuffer oldBuffer) throws IOException;

    @Override
    protected synchronized final int fillFromSource(final int minimum) throws IOException {
        this.checkClosed();

        final ByteBuffer decompressedBuffer = this.buffer;
        ByteBuffer compressedBuffer = this.compressedBuffer;
        final ZstdDecompressCtx decompressor = this.decompressor;

        for (;;) {
            final int remaining = decompressedBuffer.position();
            if (remaining >= minimum) {
                decompressedBuffer.flip();
                return minimum;
            }

            if (compressedBuffer == null || !compressedBuffer.hasRemaining()) {
                // try to read more data into source
                this.compressedBuffer = compressedBuffer = this.readFromZSTDSource(compressedBuffer);

                if (compressedBuffer == null || !compressedBuffer.hasRemaining()) {
                    // EOF
                    if (!this.lastDecompressFlushed) {
                        throw new ZstdIOException(Zstd.errCorruptionDetected(), "Truncated stream");
                    }
                    decompressedBuffer.flip();
                    return remaining;
                } else {
                    // more data to decompress, so reset the last flushed
                    this.lastDecompressFlushed = false;
                }
            }

            // set up buffer for decompression
            // old position - end of unread uncompressed data
            // old limit - end of usable buffer
            // new position - start of new uncompressed data
            // new limit - max end (excl) of new uncompressed data

            // since we do not have anything from source, try to use what remains of the input buffer
            if (compressedBuffer.hasRemaining()) {
                this.lastDecompressFlushed = decompressor.decompressDirectByteBufferStream(decompressedBuffer, compressedBuffer);
            }
        }
    }

    @Override
    protected synchronized final int readFromSource(final ByteBuffer into, int offset, int length) throws IOException {
        if (((length | offset) | (offset + length) | (into.limit() - (offset + length))) < 0) {
            // length < 0 || off < 0 || (off + len) < 0
            throw new IndexOutOfBoundsException();
        }

        if (length == 0) {
            return 0;
        }

        final ByteBuffer decompressedBuffer = this.buffer;

        if (decompressedBuffer == into) {
            throw new IllegalArgumentException("Cannot read indirectly into decompress buffer");
        }

        this.checkClosed();

        // try to read the full buffer
        if (!this.fill()) {
            return -1;
        }

        int ret = 0;
        do {
            final int position = decompressedBuffer.position();
            final int toRead = Math.min(decompressedBuffer.limit() - position, length);
            decompressedBuffer.position(position + toRead);

            into.put(offset, decompressedBuffer, position, toRead);

            ret += toRead;

            offset += toRead;
            length -= toRead;
        } while (length > 0 && this.fill());

        return ret;
    }

    @Override
    protected synchronized final long skipSource(final long n) throws IOException {
        if (n <= 0L) {
            return 0L;
        }

        this.checkClosed();

        final ByteBuffer decompressedBuffer = this.buffer;

        if (decompressedBuffer.hasRemaining()) {
            throw new IllegalStateException("Decompressed buffer must be empty");
        }

        long ret = 0L;

        if (!this.fill()) {
            return ret;
        }

        do {
            final int rem = decompressedBuffer.remaining();
            final int toAdvance = (int)Math.min((long)rem, n - ret);

            decompressedBuffer.position(decompressedBuffer.position() + toAdvance);

            ret += (long)toAdvance;
        } while (ret < n && this.fill());

        return ret;
    }

    @Override
    protected final void skipFullySource(long n) throws IOException {
        if (this.skipSource(n) < n) {
            throw new EOFException();
        }
    }

    protected abstract long availableZSTDSource() throws IOException;

    @Override
    protected synchronized final long availableSource() throws IOException {
        if (this.closed) {
            return 0L;
        }

        final ByteBuffer compressedBuffer = this.compressedBuffer;
        return (compressedBuffer.limit() - compressedBuffer.position()) + this.availableZSTDSource();
    }

    protected abstract void closeZSTDSource() throws IOException;

    @Override
    protected synchronized final void closeSource() throws IOException {
        final ZstdDecompressCtx decompressor = this.decompressor;

        this.closed = true;
        this.compressedBuffer = null;
        this.decompressor = null;

        try {
            this.closeZSTDSource();
        } finally {
            if (this.closeDecompressor != null) {
                this.closeDecompressor.accept(decompressor);
            }
        }
    }
}
