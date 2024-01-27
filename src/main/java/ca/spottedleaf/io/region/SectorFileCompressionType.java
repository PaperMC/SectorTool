package ca.spottedleaf.io.region;

import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferInputStream;
import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferOutputStream;
import ca.spottedleaf.io.region.io.java.SimpleBufferedInputStream;
import ca.spottedleaf.io.region.io.java.SimpleBufferedOutputStream;
import ca.spottedleaf.io.stream.compat.FromJavaInputStream;
import ca.spottedleaf.io.stream.compat.FromJavaOutputStream;
import ca.spottedleaf.io.stream.compat.ToJavaInputStream;
import ca.spottedleaf.io.stream.compat.ToJavaOutputStream;
import ca.spottedleaf.io.buffer.BufferChoices;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

public abstract class SectorFileCompressionType {

    private static final Int2ObjectMap<SectorFileCompressionType> BY_ID = Int2ObjectMaps.synchronize(new Int2ObjectOpenHashMap<>());

    public static final SectorFileCompressionType GZIP = new SectorFileCompressionType(1) {
        @Override
        public InputStream createInput(final BufferChoices scopedBufferChoices, final ByteBufferInputStream input) throws IOException {
            return new SimpleBufferedInputStream(new GZIPInputStream(input), scopedBufferChoices.t16k().acquireJavaBuffer());
        }

        @Override
        public OutputStream createOutput(final BufferChoices scopedBufferChoices, final ByteBufferOutputStream output) throws IOException {
            return new SimpleBufferedOutputStream(new GZIPOutputStream(output), scopedBufferChoices.t16k().acquireJavaBuffer());
        }
    };
    public static final SectorFileCompressionType DEFLATE = new SectorFileCompressionType(2) {
        @Override
        public InputStream createInput(final BufferChoices scopedBufferChoices, final ByteBufferInputStream input) throws IOException {
            return new SimpleBufferedInputStream(new InflaterInputStream(input), scopedBufferChoices.t16k().acquireJavaBuffer());
        }

        @Override
        public OutputStream createOutput(final BufferChoices scopedBufferChoices, final ByteBufferOutputStream output) throws IOException {
            return new SimpleBufferedOutputStream(new DeflaterOutputStream(output), scopedBufferChoices.t16k().acquireJavaBuffer());
        }
    };
    public static final SectorFileCompressionType NONE = new SectorFileCompressionType(3) {
        @Override
        public InputStream createInput(final BufferChoices scopedBufferChoices, final ByteBufferInputStream input) throws IOException {
            return input;
        }

        @Override
        public OutputStream createOutput(final BufferChoices scopedBufferChoices, final ByteBufferOutputStream output) throws IOException {
            return output;
        }
    };
    public static final SectorFileCompressionType LZ4 = new SectorFileCompressionType(4) {
        @Override
        public InputStream createInput(final BufferChoices scopedBufferChoices, final ByteBufferInputStream input) throws IOException {
            return new SimpleBufferedInputStream(new LZ4BlockInputStream(input), scopedBufferChoices.t16k().acquireJavaBuffer());
        }

        @Override
        public OutputStream createOutput(final BufferChoices scopedBufferChoices, final ByteBufferOutputStream output) throws IOException {
            return new SimpleBufferedOutputStream(new LZ4BlockOutputStream(output), scopedBufferChoices.t16k().acquireJavaBuffer());
        }
    };
    public static final SectorFileCompressionType ZSTD = new SectorFileCompressionType(5) {
        @Override
        public InputStream createInput(final BufferChoices scopedBufferChoices, final ByteBufferInputStream input) throws IOException {
            return new ToJavaInputStream(
                new ca.spottedleaf.io.stream.databuffered.zstd.wrapped.WrappedZSTDBufferedDataByteBufferInputStream(
                    scopedBufferChoices.t16k().acquireDirectBuffer(), scopedBufferChoices.zstdCtxs().acquireDecompressor(),
                    new FromJavaInputStream(input, scopedBufferChoices.t16k().acquireJavaBuffer()),
                    scopedBufferChoices.t16k().acquireDirectBuffer(), null
                ), scopedBufferChoices.t16k().acquireDirectBuffer()
            );
        }

        @Override
        public OutputStream createOutput(final BufferChoices scopedBufferChoices, final ByteBufferOutputStream output) throws IOException {
            return new ToJavaOutputStream(
                new ca.spottedleaf.io.stream.databuffered.zstd.wrapped.WrappedZSTDBufferedDataByteBufferOutputStream(
                    scopedBufferChoices.t16k().acquireDirectBuffer(), scopedBufferChoices.zstdCtxs().acquireCompressor(),
                    new FromJavaOutputStream(output, scopedBufferChoices.t16k().acquireJavaBuffer()),
                    scopedBufferChoices.t16k().acquireDirectBuffer(), null
                ), scopedBufferChoices.t16k().acquireDirectBuffer()
            );
        }
    };

    private final int id;

    protected SectorFileCompressionType(final int id) {
        this.id = id;
        if (BY_ID.putIfAbsent(id, this) != null) {
            throw new IllegalArgumentException("Duplicate id");
        }
    }

    public final int getId() {
        return this.id;
    }

    public abstract InputStream createInput(final BufferChoices scopedBufferChoices, final ByteBufferInputStream input) throws IOException;

    public abstract OutputStream createOutput(final BufferChoices scopedBufferChoices, final ByteBufferOutputStream output) throws IOException;

    public static SectorFileCompressionType getById(final int id) {
        return BY_ID.get(id);
    }
}
