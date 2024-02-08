package ca.spottedleaf.sectortool.stream.databuffered.zstd.wrapped;

import ca.spottedleaf.sectortool.stream.ByteBufferInputStream;
import ca.spottedleaf.sectortool.stream.databuffered.zstd.AbstractZSTDBufferedDataByteBufferInputStream;
import com.github.luben.zstd.ZstdDecompressCtx;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class WrappedZSTDBufferedDataByteBufferInputStream extends AbstractZSTDBufferedDataByteBufferInputStream {

    protected final ByteBufferInputStream wrap;

    public WrappedZSTDBufferedDataByteBufferInputStream(final ByteBufferInputStream wrap) {
        this(ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE), new ZstdDecompressCtx(), wrap, ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE));
    }

    public WrappedZSTDBufferedDataByteBufferInputStream(final ByteBuffer decompressedBuffer, final ByteBufferInputStream wrap) {
        this(decompressedBuffer, new ZstdDecompressCtx(), wrap, ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE));
    }

    public WrappedZSTDBufferedDataByteBufferInputStream(final ByteBuffer decompressedBuffer, final ByteBufferInputStream wrap,
                                                        final ByteBuffer compressedBuffer) {
        this(decompressedBuffer, new ZstdDecompressCtx(), wrap, compressedBuffer);
    }

    public WrappedZSTDBufferedDataByteBufferInputStream(final ByteBuffer decompressedBuffer, final ZstdDecompressCtx decompressor,
                                                        final ByteBufferInputStream wrap, final ByteBuffer compressedBuffer) {
        this(decompressedBuffer, decompressor, wrap, compressedBuffer, ZstdDecompressCtx::close);
    }

    public WrappedZSTDBufferedDataByteBufferInputStream(final ByteBuffer decompressedBuffer, final ZstdDecompressCtx decompressor,
                                                        final ByteBufferInputStream wrap, final ByteBuffer compressedBuffer,
                                                        final Consumer<ZstdDecompressCtx> closeDecompressor) {
        super(decompressedBuffer, decompressor, closeDecompressor);

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
        compressedBuffer.position(compressedBuffer.capacity());

        // do not rely on proper publishing
        synchronized (this) {
            this.wrap = wrap;
            this.compressedBuffer = compressedBuffer;
        }
    }

    @Override
    protected ByteBuffer readFromZSTDSource(final ByteBuffer oldBuffer) throws IOException {
        final ByteBuffer buffer = this.compressedBuffer;
        buffer.limit(buffer.capacity());
        buffer.position(0);

        try {
            final int read = this.wrap.read(buffer, 0, buffer.capacity());
            if (read > 0) {
                buffer.position(read);
            }
        } finally {
            // if read throws, we will update buffer limit to 0 (ensuring remaining = 0)
            buffer.flip();
        }

        return buffer;
    }

    @Override
    protected long availableZSTDSource() throws IOException {
        return this.wrap.available();
    }

    @Override
    protected void closeZSTDSource() throws IOException {
        this.wrap.close();
    }
}
