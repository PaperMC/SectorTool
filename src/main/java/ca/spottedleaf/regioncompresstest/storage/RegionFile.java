package ca.spottedleaf.regioncompresstest.storage;

import ca.spottedleaf.io.region.io.bytebuffer.BufferedFileChannelInputStream;
import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferInputStream;
import ca.spottedleaf.io.buffer.BufferChoices;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import net.jpountz.lz4.LZ4BlockInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

/**
 * Read only support of the region file format used by Minecraft
 */
public final class RegionFile implements Closeable {

    /* Based on https://github.com/JosuaKrause/NBTEditor/blob/master/src/net/minecraft/world/level/chunk/storage/RegionFile.java */

    public static final String ANVIL_EXTENSION = ".mca";
    public static final String MCREGION_EXTENSION = ".mcr";

    private static final int SECTOR_SHIFT = 12;
    private static final int SECTOR_SIZE = 1 << SECTOR_SHIFT;

    private static final int BYTE_SIZE   = Byte.BYTES;
    private static final int SHORT_SIZE  = Short.BYTES;
    private static final int INT_SIZE    = Integer.BYTES;
    private static final int LONG_SIZE   = Long.BYTES;
    private static final int FLOAT_SIZE  = Float.BYTES;
    private static final int DOUBLE_SIZE = Double.BYTES;

    private static final int DATA_METADATA_SIZE = BYTE_SIZE + INT_SIZE;

    private final int[] header = new int[SECTOR_SIZE / INT_SIZE];
    private final int[] timestamps = new int[SECTOR_SIZE / INT_SIZE];

    public final File file;
    public final int sectionX;
    public final int sectionZ;

    private FileChannel channel;

    public RegionFile(final File file, final int sectionX, final int sectionZ, final BufferChoices unscopedBufferChoices) throws IOException {
        this.file = file;
        this.sectionX = sectionX;
        this.sectionZ = sectionZ;
        this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);

        if (this.channel.size() < (2L * (long)SECTOR_SIZE)) {
            System.err.println("Truncated header in file: " + file.getAbsolutePath());
            return;
        }

        try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
            final ByteBuffer headerBuffer = scopedBufferChoices.t16k().acquireDirectBuffer();

            headerBuffer.order(ByteOrder.BIG_ENDIAN);
            headerBuffer.limit(2 * SECTOR_SIZE);
            headerBuffer.position(0);
            this.channel.read(headerBuffer, 0L);
            headerBuffer.flip();

            final IntBuffer headerIntBuffer = headerBuffer.asIntBuffer();

            headerIntBuffer.get(0, this.header, 0, SECTOR_SIZE / INT_SIZE);
            headerIntBuffer.get(0 + SECTOR_SIZE / INT_SIZE, this.timestamps, 0, SECTOR_SIZE / INT_SIZE);
        }
    }

    private static int getLocationSize(final int location) {
        return location & 255;
    }

    private static int getLocationOffset(final int location) {
        return location >>> 8;
    }

    public boolean has(final int x, final int z) {
        return this.getLocation(x, z) != 0;
    }

    private File getExternalFile(final int x, final int z) {
        final int cx = (x & 31) | (this.sectionX << 5);
        final int cz = (z & 31) | (this.sectionZ << 5);

        return new File(this.file.getParentFile(), "c." + cx + "." + cz + ".mcc");
    }

    public static record AllocationStats(long fileSectors, long allocatedSectors, long alternateAllocatedSectors, long dataSizeBytes, int errors) {}

    private int[] getHeaderSorted() {
        final IntArrayList list = new IntArrayList(this.header.length);
        for (final int location : this.header) {
            list.add(location);
        }

        list.sort((a, b) -> Integer.compare(getLocationOffset(a), getLocationOffset(b)));

        return list.toArray(new int[this.header.length]);
    }

    public AllocationStats computeStats(final BufferChoices unscopedBufferChoices, final int alternateSectorSize,
                                        final int alternateOverhead) throws IOException {
        final long fileSectors = ((this.file.length() + (SECTOR_SIZE - 1)) >> SECTOR_SHIFT);

        long allocatedSectors = Math.min(fileSectors, 2L);
        long alternateAllocatedSectors = 0L;
        int errors = 0;
        long dataSize = 0L;

        try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
            final ByteBuffer ioBuffer = scopedBufferChoices.t16k().acquireDirectBuffer();

            for (final int location : this.getHeaderSorted()) {
                if (location == 0) {
                    continue;
                }

                final int offset = getLocationOffset(location);
                final int size = getLocationSize(location);

                if (offset <= 1 || size <= 0 || (offset + size) > fileSectors) {
                    // invalid
                    ++errors;
                    continue;
                }

                ioBuffer.limit(INT_SIZE);
                ioBuffer.position(0);

                this.channel.read(ioBuffer, (long)offset << SECTOR_SHIFT);

                if (ioBuffer.hasRemaining()) {
                    ++errors;
                    continue;
                }

                final int rawSize = ioBuffer.getInt(0) + INT_SIZE;

                final int rawEnd = rawSize + (offset << SECTOR_SHIFT);
                if (rawSize <= 0 || rawEnd <= 0 || ((rawSize + (SECTOR_SIZE - 1)) >> SECTOR_SHIFT) > fileSectors) {
                    ++errors;
                    continue;
                }

                final int compressedSize = rawSize - (INT_SIZE + BYTE_SIZE);

                final int alternateRawSize = (compressedSize + alternateOverhead);

                // support forge oversized by using data size
                allocatedSectors += (long)((rawSize + (SECTOR_SIZE - 1)) >> SECTOR_SHIFT);
                dataSize += (long)compressedSize;
                alternateAllocatedSectors += (long)((alternateRawSize + (alternateSectorSize - 1)) / alternateSectorSize);
            }
        }

        return new AllocationStats(fileSectors, allocatedSectors, alternateAllocatedSectors, dataSize, errors);
    }

    public boolean read(final int x, final int z, final BufferChoices unscopedBufferChoices, final RegionFile.CustomByteArrayOutputStream decompressed) throws IOException {
        final int location = this.getLocation(x, z);

        if (location == 0) {
            // absent
            return false;
        }

        try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
            ByteBuffer compressedData = scopedBufferChoices.t1m().acquireDirectBuffer();

            final long foff = ((long)getLocationOffset(location) * (long)SECTOR_SIZE);
            int fsize = (getLocationSize(location) * SECTOR_SIZE);

            if (fsize == (255 * SECTOR_SIZE)) {
                // support for forge/spigot style oversized chunk format (pre 1.15)
                final ByteBuffer extendedLen = ByteBuffer.allocate(INT_SIZE);
                this.channel.read(extendedLen, foff);
                fsize = extendedLen.getInt(0);
                if (fsize > compressedData.capacity()) {
                    // do not use direct here, the read() will allocate one and free it immediately - which is something
                    // we cannot do with standard API
                    compressedData = ByteBuffer.allocate(fsize);
                }
            }

            compressedData.order(ByteOrder.BIG_ENDIAN);
            compressedData.limit(fsize);
            compressedData.position(0);

            int r = this.channel.read(compressedData, foff);
            if (r < DATA_METADATA_SIZE) {
                throw new IOException("Truncated data");
            }
            compressedData.flip();


            final int length = compressedData.getInt(0) - BYTE_SIZE;
            byte type = compressedData.get(0 + INT_SIZE);
            compressedData.position(0 + INT_SIZE + BYTE_SIZE);

            if (compressedData.remaining() < length) {
                throw new EOFException("Truncated data");
            }

            final InputStream rawIn;
            if ((type & 128) != 0) {
                // stored externally
                type = (byte)((int)type & 127);

                final File external = this.getExternalFile(x, z);
                if (!external.isFile()) {
                    System.err.println("Externally stored chunk data '" + external.getAbsolutePath() + "' does not exist");
                    return false;
                }

                rawIn = new BufferedFileChannelInputStream(scopedBufferChoices.t16k().acquireDirectBuffer(), this.getExternalFile(x, z));
            } else {
                rawIn = new ByteBufferInputStream(compressedData);
            }

            InputStream decompress = null;
            try {
                switch (type) {
                    case 1: { // GZIP
                        decompress = new GZIPInputStream(rawIn);
                        break;
                    }
                    case 2: { // DEFLATE
                        decompress = new InflaterInputStream(rawIn);
                        break;
                    }
                    case 3: { // NONE
                        decompress = rawIn;
                        break;
                    }
                    case 4: { // LZ4
                        decompress = new LZ4BlockInputStream(rawIn);
                        break;
                    }
                    default: {
                        throw new IOException("Unknown type: " + type);
                    }
                }

                final byte[] tmp = scopedBufferChoices.t16k().acquireJavaBuffer();

                while ((r = decompress.read(tmp)) >= 0) {
                    decompressed.write(tmp, 0, r);
                }

                return true;
            } finally {
                if (decompress != null) {
                    decompress.close();
                }
            }
        }
    }

    private static int makeIndex(final int x, final int z) {
        return (x & 31) | ((z & 31) << 5);
    }

    public int getLocation(final int x, final int z) {
        return this.header[makeIndex(x, z)];
    }

    public int getTimestamp(final int x, final int z) {
        return this.timestamps[makeIndex(x, z)];
    }

    @Override
    public synchronized void close() throws IOException {
        final FileChannel channel = this.channel;
        if (channel != null) {
            this.channel = null;
            channel.close();
        }
    }

    public static final class CustomByteArrayOutputStream extends ByteArrayOutputStream {

        public CustomByteArrayOutputStream(final byte[] bytes) {
            super(0);
            this.buf = bytes;
        }

        public byte[] getBuffer() {
            return this.buf;
        }
    }
}
