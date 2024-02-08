package ca.spottedleaf.sectortool.storage;

import ca.spottedleaf.io.region.SectorFile;
import ca.spottedleaf.io.region.io.bytebuffer.BufferedFileChannelInputStream;
import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferInputStream;
import ca.spottedleaf.io.buffer.BufferChoices;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Read/write support of the region file format used by Minecraft
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
    private final boolean readOnly;
    private final SectorFile.SectorAllocator sectorAllocator = new SectorFile.SectorAllocator(getLocationOffset(-1) - 1, getLocationSize(-1));
    {
        this.sectorAllocator.allocate(2, false);
    }

    public RegionFile(final File file, final int sectionX, final int sectionZ, final BufferChoices unscopedBufferChoices,
                      final boolean readOnly) throws IOException {
        this.file = file;
        this.sectionX = sectionX;
        this.sectionZ = sectionZ;
        this.readOnly = readOnly;

        if (readOnly) {
            this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } else {
            file.getParentFile().mkdirs();
            this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        }

        final long size = this.channel.size();

        if (size != 0L) {
            if (size < (2L * (long)SECTOR_SIZE)) {
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

                for (int i = 0; i < this.header.length; ++i) {
                    final int location = this.header[i];

                    if (location == 0) {
                        continue;
                    }

                    if (!this.sectorAllocator.tryAllocateDirect(getLocationOffset(location), getLocationSize(location), false)) {
                        this.header[i] = 0;
                        System.err.println("Invalid sector allocation in regionfile '" + this.file.getAbsolutePath() + "': (" + i + "," + location + ")");
                    }
                }
            }
        }
    }

    private static int getLocationSize(final int location) {
        return location & 255;
    }

    private static int getLocationOffset(final int location) {
        return location >>> 8;
    }

    private static int makeLocation(final int offset, final int size) {
        return (offset << 8) | (size & 255);
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
        final long fileSectors = (this.file.length() + (SECTOR_SIZE - 1)) >> SECTOR_SHIFT;

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

    private void writeHeader(final BufferChoices unscopedBufferChoices) throws IOException {
        try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
            final ByteBuffer buffer = scopedBufferChoices.t16k().acquireDirectBuffer();

            buffer.limit((this.header.length + this.timestamps.length) * INT_SIZE);
            buffer.position(0);

            buffer.asIntBuffer().put(this.header).put(this.timestamps);

            this.channel.write(buffer, 0L << SECTOR_SHIFT);
        }
    }

    public void delete(final int x, final int z, final BufferChoices unscopedBufferChoices) throws IOException {
        final int location = this.getLocation(x, z);
        if (location == 0) {
            return;
        }

        this.setLocation(x, z, 0);
        this.setTimestamp(x, z, 0);
        this.getExternalFile(x, z).delete();

        this.writeHeader(unscopedBufferChoices);
        this.sectorAllocator.freeAllocation(getLocationOffset(location), getLocationSize(location));
    }

    public void write(final int x, final int z, final BufferChoices unscopedBufferChoices, final int compressType,
                      final byte[] data, final int off, final int len) throws IOException {
        try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
            final CustomByteArrayOutputStream rawStream = new CustomByteArrayOutputStream(scopedBufferChoices.t1m().acquireJavaBuffer());

            for (int i = 0; i < (INT_SIZE + BYTE_SIZE); ++i) {
                rawStream.write(0);
            }

            final OutputStream out;
            switch (compressType) {
                case 1: { // GZIP
                    out = new GZIPOutputStream(rawStream);
                    break;
                }
                case 2: { // DEFLATE
                    out = new DeflaterOutputStream(rawStream);
                    break;
                }
                case 3: { // NONE
                    out = rawStream;
                    break;
                }
                case 4: { // LZ4
                    out = new LZ4BlockOutputStream(rawStream);
                    break;
                }
                default: {
                    throw new IOException("Unknown type: " + compressType);
                }
            }

            out.write(data, off, len);
            out.close();

            ByteBuffer.wrap(rawStream.getBuffer(), 0, rawStream.size())
                    .putInt(0, rawStream.size() - INT_SIZE)
                    .put(INT_SIZE, (byte)compressType);

            final int requiredSectors = (rawStream.size() + (SECTOR_SIZE - 1)) >> SECTOR_SHIFT;
            final ByteBuffer write;

            if (requiredSectors <= 255) {
                write = ByteBuffer.wrap(rawStream.getBuffer(), 0, rawStream.size());
            } else {
                write = ByteBuffer.allocate(INT_SIZE + BYTE_SIZE);
                write.putInt(0, 1);
                write.put(INT_SIZE, (byte)(128 | compressType));
            }

            final int internalSectors = (write.remaining() + (SECTOR_SIZE - 1)) >> SECTOR_SHIFT;
            final int internalOffset = this.sectorAllocator.allocate(internalSectors, true);
            if (internalOffset <= 0) {
                throw new IllegalStateException("Failed to allocate internally");
            }

            this.channel.write(write, (long)internalOffset << SECTOR_SHIFT);

            if (requiredSectors > 255) {
                final File external = this.getExternalFile(x, z);

                System.out.println("Storing " + rawStream.size() + " bytes to " + external.getAbsolutePath());

                final File externalTmp = new File(external.getParentFile(), external.getName() + ".tmp");
                externalTmp.delete();
                externalTmp.createNewFile();
                try (final FileOutputStream fout = new FileOutputStream(externalTmp)) {
                    fout.write(rawStream.getBuffer(), INT_SIZE + BYTE_SIZE, rawStream.size() - (INT_SIZE + BYTE_SIZE));
                }

                try {
                    Files.move(externalTmp.toPath(), external.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (final AtomicMoveNotSupportedException ex) {
                    Files.move(externalTmp.toPath(), external.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
            } else {
                this.getExternalFile(x, z).delete();
            }

            final int oldLocation = this.getLocation(x, z);

            this.setLocation(x, z, makeLocation(internalOffset, internalSectors));
            this.setTimestamp(x, z, (int)(System.currentTimeMillis() / 1000L));

            this.writeHeader(unscopedBufferChoices);

            if (oldLocation != 0) {
                this.sectorAllocator.freeAllocation(getLocationOffset(oldLocation), getLocationSize(oldLocation));
            }
        }
    }

    private static int makeIndex(final int x, final int z) {
        return (x & 31) | ((z & 31) << 5);
    }

    public int getLocation(final int x, final int z) {
        return this.header[makeIndex(x, z)];
    }

    private void setLocation(final int x, final int z, final int location) {
        this.header[makeIndex(x, z)] = location;
    }

    public int getTimestamp(final int x, final int z) {
        return this.timestamps[makeIndex(x, z)];
    }

    private void setTimestamp(final int x, final int z, final int time) {
        this.timestamps[makeIndex(x, z)] = time;
    }

    @Override
    public synchronized void close() throws IOException {
        final FileChannel channel = this.channel;
        if (channel != null) {
            this.channel = null;
            if (this.readOnly) {
                channel.close();
            } else {
                try {
                    channel.force(true);
                } finally {
                    channel.close();
                }
            }
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
