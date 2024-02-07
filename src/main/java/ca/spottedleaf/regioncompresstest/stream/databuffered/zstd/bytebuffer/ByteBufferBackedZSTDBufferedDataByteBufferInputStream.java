package ca.spottedleaf.regioncompresstest.stream.databuffered.zstd.bytebuffer;

import ca.spottedleaf.regioncompresstest.stream.databuffered.zstd.AbstractZSTDBufferedDataByteBufferInputStream;
import com.github.luben.zstd.ZstdDecompressCtx;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

// this class is sneaky; simply making the "compressed buffer" of the superclass the full compressed data passed to constructor
public class ByteBufferBackedZSTDBufferedDataByteBufferInputStream extends AbstractZSTDBufferedDataByteBufferInputStream {

    public ByteBufferBackedZSTDBufferedDataByteBufferInputStream(final ByteBuffer compressed) {
        this(ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE), new ZstdDecompressCtx(), compressed);
    }

    public ByteBufferBackedZSTDBufferedDataByteBufferInputStream(final ByteBuffer decompressedBuffer, final ByteBuffer compressed) {
        this(decompressedBuffer, new ZstdDecompressCtx(), compressed);
    }

    public ByteBufferBackedZSTDBufferedDataByteBufferInputStream(final ByteBuffer decompressedBuffer, final ZstdDecompressCtx decompressor,
                                                                 final ByteBuffer compressed) {
        this(decompressedBuffer, decompressor, compressed, ZstdDecompressCtx::close);
    }

    public ByteBufferBackedZSTDBufferedDataByteBufferInputStream(final ByteBuffer decompressedBuffer, final ZstdDecompressCtx decompressor,
                                                                 final ByteBuffer compressed, final Consumer<ZstdDecompressCtx> closeDecompressor) {
        super(decompressedBuffer, decompressor, closeDecompressor);

        if (compressed == null) {
            throw new NullPointerException("Compressed buffer is null");
        }
        if (!compressed.isDirect()) {
            throw new IllegalArgumentException("Compressed buffer must be direct");
        }

        // do not rely on proper publishing
        synchronized (this) {
            this.compressedBuffer = compressed;
        }
    }

    @Override
    protected ByteBuffer readFromZSTDSource(final ByteBuffer oldBuffer) throws IOException {
        return oldBuffer;
    }

    @Override
    protected long availableZSTDSource() throws IOException {
        return 0L;
    }

    @Override
    protected void closeZSTDSource() throws IOException {}
}
