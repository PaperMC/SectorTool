package ca.spottedleaf.regioncompresstest.stream.databuffered.zstd.wrapped;

import ca.spottedleaf.regioncompresstest.stream.ByteBufferOutputStream;
import ca.spottedleaf.regioncompresstest.stream.databuffered.AbstractBufferedDataByteBufferOutputStream;
import ca.spottedleaf.regioncompresstest.stream.databuffered.zstd.AbstractZSTDBufferedDataByteBufferOutputStream;
import com.github.luben.zstd.ZstdCompressCtx;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class WrappedZSTDBufferedDataByteBufferOutputStream extends AbstractZSTDBufferedDataByteBufferOutputStream {

    protected final ByteBufferOutputStream wrap;

    public WrappedZSTDBufferedDataByteBufferOutputStream(final ByteBufferOutputStream wrap) {
        this(
                ByteBuffer.allocateDirect(AbstractBufferedDataByteBufferOutputStream.DEFAULT_BUFFER_SIZE),
                new ZstdCompressCtx(), wrap,
                ByteBuffer.allocateDirect(AbstractBufferedDataByteBufferOutputStream.DEFAULT_BUFFER_SIZE)
        );
    }

    public WrappedZSTDBufferedDataByteBufferOutputStream(final ByteBuffer decompressedBuffer, final ByteBufferOutputStream wrap) {
        this(
                decompressedBuffer,
                new ZstdCompressCtx(), wrap,
                ByteBuffer.allocateDirect(AbstractBufferedDataByteBufferOutputStream.DEFAULT_BUFFER_SIZE)
        );
    }

    public WrappedZSTDBufferedDataByteBufferOutputStream(final ByteBuffer decompressedBuffer,
                                                         final ZstdCompressCtx compressor, final ByteBufferOutputStream wrap) {
        this(
                decompressedBuffer, compressor, wrap,
                ByteBuffer.allocateDirect(AbstractBufferedDataByteBufferOutputStream.DEFAULT_BUFFER_SIZE)
        );
    }

    public WrappedZSTDBufferedDataByteBufferOutputStream(final ByteBuffer decompressedBuffer,
                                                         final ZstdCompressCtx compressor,
                                                         final ByteBufferOutputStream wrap,
                                                         final ByteBuffer compressedBuffer) {
        this(decompressedBuffer, compressor, wrap, compressedBuffer, ZstdCompressCtx::close);
    }
    public WrappedZSTDBufferedDataByteBufferOutputStream(final ByteBuffer decompressedBuffer,
                                                         final ZstdCompressCtx compressor,
                                                         final ByteBufferOutputStream wrap,
                                                         final ByteBuffer compressedBuffer,
                                                         final Consumer<ZstdCompressCtx> closeCompressor) {
        super(decompressedBuffer, compressor, closeCompressor);

        if (wrap == null) {
            throw new NullPointerException("Wrapped stream is null");
        }

        if (compressedBuffer == null) {
            throw new NullPointerException("Compressed buffer is null");
        }
        if (!compressedBuffer.isDirect()) {
            throw new IllegalArgumentException("Compressed buffer must be direct");
        }

        compressedBuffer.limit(compressedBuffer.capacity());
        compressedBuffer.position(0);

        // do not rely on proper publishing
        synchronized (this) {
            this.wrap = wrap;
            this.compressedBuffer = compressedBuffer;
        }
    }

    @Override
    protected ByteBuffer writeZSTDSource(final ByteBuffer buffer) throws IOException {
        this.wrap.write(buffer, buffer.position(), buffer.limit() - buffer.position());

        buffer.limit(buffer.capacity());
        buffer.position(0);
        return buffer;
    }

    @Override
    protected void flushZSTDDestination() throws IOException {
        this.wrap.flush();
    }

    @Override
    protected void closeZSTDDestination() throws IOException {
        this.wrap.close();
    }
}
