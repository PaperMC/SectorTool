package ca.spottedleaf.io.region.io.zstd;

import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferOutputStream;
import com.github.luben.zstd.EndDirective;
import com.github.luben.zstd.ZstdCompressCtx;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class ZSTDOutputStream extends ByteBufferOutputStream {

    protected static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0);

    private ByteBuffer compressedBuffer;
    private ZstdCompressCtx compressor;
    private Consumer<ZstdCompressCtx> closeCompressor;
    private ByteBufferOutputStream wrap;

    public ZSTDOutputStream(final ByteBuffer decompressedBuffer, final ByteBuffer compressedBuffer,
                            final ZstdCompressCtx compressor,
                            final Consumer<ZstdCompressCtx> closeCompressor,
                            final ByteBufferOutputStream wrap) {
        super(decompressedBuffer);

        if (!decompressedBuffer.isDirect() || !compressedBuffer.isDirect()) {
            throw new IllegalArgumentException("Buffers must be direct");
        }

        decompressedBuffer.limit(decompressedBuffer.capacity());
        decompressedBuffer.position(0);

        compressedBuffer.limit(compressedBuffer.capacity());
        compressedBuffer.position(0);

        synchronized (this) {
            this.compressedBuffer = compressedBuffer;
            this.compressor = compressor;
            this.closeCompressor = closeCompressor;
            this.wrap = wrap;
        }
    }

    protected synchronized ByteBuffer emptyBuffer(final ByteBuffer toFlush) throws IOException {
        toFlush.flip();

        if (toFlush.hasRemaining()) {
            this.wrap.write(toFlush);
        }

        toFlush.limit(toFlush.capacity());
        toFlush.position(0);

        return toFlush;
    }

    @Override
    protected synchronized final ByteBuffer flush(final ByteBuffer current) throws IOException {
        current.flip();

        while (current.hasRemaining()) {
            if (!this.compressedBuffer.hasRemaining()) {
                this.compressedBuffer = this.emptyBuffer(this.compressedBuffer);
            }
            this.compressor.compressDirectByteBufferStream(this.compressedBuffer, current, EndDirective.CONTINUE);
        }

        current.limit(current.capacity());
        current.position(0);

        return current;
    }

    @Override
    public synchronized void flush() throws IOException {
        // flush all buffered data to zstd stream first
        super.flush();

        // now try to dump compressor buffers
        do {
            if (!this.compressedBuffer.hasRemaining()) {
                this.compressedBuffer = this.emptyBuffer(this.compressedBuffer);
            }
        } while (!this.compressor.compressDirectByteBufferStream(this.compressedBuffer, EMPTY_BUFFER, EndDirective.FLUSH));

        // empty compressed buffer into wrap
        if (this.compressedBuffer.position() != 0) {
            this.compressedBuffer = this.emptyBuffer(this.compressedBuffer);
        }

        this.wrap.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.compressor == null) {
            // already closed
            return;
        }

        try {
            // flush data to compressor
            try {
                super.flush();
            } finally {
                // perform super.close
                // the reason we inline this is so that we do not call our flush(), so that we do not perform ZSTD FLUSH + END,
                // which is slightly more inefficient than just END
                this.buffer = null;
            }

            // perform end stream
            do {
                if (!this.compressedBuffer.hasRemaining()) {
                    this.compressedBuffer = this.emptyBuffer(this.compressedBuffer);
                }
            } while (!this.compressor.compressDirectByteBufferStream(this.compressedBuffer, EMPTY_BUFFER, EndDirective.END));

            // flush compressed buffer
            if (this.compressedBuffer.position() != 0) {
                this.compressedBuffer = this.emptyBuffer(this.compressedBuffer);
            }

            // try-finally will flush wrap
        } finally {
            try {
                if (this.closeCompressor != null) {
                    this.closeCompressor.accept(this.compressor);
                }
            } finally {
                try {
                    this.wrap.close();
                } finally {
                    this.compressor = null;
                    this.closeCompressor = null;
                    this.compressedBuffer = null;
                    this.wrap = null;
                }
            }
        }
    }
}
