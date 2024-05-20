package ca.spottedleaf.io.region;

import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferInputStream;
import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferOutputStream;
import ca.spottedleaf.io.region.io.java.SimpleBufferedInputStream;
import ca.spottedleaf.io.region.io.java.SimpleBufferedOutputStream;
import ca.spottedleaf.io.region.io.zstd.ZSTDInputStream;
import ca.spottedleaf.io.region.io.zstd.ZSTDOutputStream;
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
            return new ZSTDInputStream(
                    scopedBufferChoices.t16k().acquireDirectBuffer(), scopedBufferChoices.t16k().acquireDirectBuffer(),
                    scopedBufferChoices.zstdCtxs().acquireDecompressor(), null, input
            );
        }

        @Override
        public OutputStream createOutput(final BufferChoices scopedBufferChoices, final ByteBufferOutputStream output) throws IOException {
            return new ZSTDOutputStream(
                    scopedBufferChoices.t16k().acquireDirectBuffer(), scopedBufferChoices.t16k().acquireDirectBuffer(),
                    scopedBufferChoices.zstdCtxs().acquireCompressor(), null, output
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

    public static SectorFileCompressionType fromRegionFile(final int id) {
        switch (id) {
            case 1: { // GZIP
                return SectorFileCompressionType.GZIP;
            }
            case 2: { // DEFLATE
                return SectorFileCompressionType.DEFLATE;
            }
            case 3: { // NONE
                return SectorFileCompressionType.NONE;
            }
            case 4: { // LZ4
                return SectorFileCompressionType.LZ4;
            }
            default: {
                throw new IllegalArgumentException();
            }
        }
    }
}
