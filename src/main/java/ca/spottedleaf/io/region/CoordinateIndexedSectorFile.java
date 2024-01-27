package ca.spottedleaf.io.region;

import ca.spottedleaf.io.region.io.bytebuffer.BufferedFileChannelInputStream;
import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferInputStream;
import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferOutputStream;
import ca.spottedleaf.io.buffer.BufferChoices;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public final class CoordinateIndexedSectorFile implements Closeable {

    private static final XXHashFactory XXHASH_FACTORY = XXHashFactory.fastestInstance();
    private static final XXHashFactory XXHASH_JAVA_FACTORY = XXHashFactory.fastestJavaInstance();
    private static final XXHash64 XXHASH64 = XXHASH_FACTORY.hash64();
    private static final long XXHASH_SEED = 0L;

    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinateIndexedSectorFile.class);

    private static final VarHandle DIRECT_READ_SHORT  = MethodHandles.byteBufferViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle DIRECT_READ_CHAR   = MethodHandles.byteBufferViewVarHandle(char[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle DIRECT_READ_INT    = MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle DIRECT_READ_LONG   = MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle DIRECT_READ_FLOAT  = MethodHandles.byteBufferViewVarHandle(float[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle DIRECT_READ_DOUBLE = MethodHandles.byteBufferViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);

    private static final int BYTE_SIZE   = Byte.BYTES;
    private static final int SHORT_SIZE  = Short.BYTES;
    private static final int INT_SIZE    = Integer.BYTES;
    private static final int LONG_SIZE   = Long.BYTES;
    private static final int FLOAT_SIZE  = Float.BYTES;
    private static final int DOUBLE_SIZE = Double.BYTES;

    public static final String FILE_EXTENSION              = ".cisf";
    public static final String FILE_EXTERNAL_EXTENSION     = ".cisfe";
    public static final String FILE_EXTERNAL_TMP_EXTENSION = FILE_EXTERNAL_EXTENSION + ".tmp";

    public static String getFileName(final int sectionX, final int sectionZ) {
        return sectionX + "." + sectionZ + FILE_EXTENSION;
    }

    public static String getExternalFileName(final int sectionX, final int sectionZ,
                                             final int localX, final int localZ) {
        final int absoluteX = (sectionX << SECTION_SHIFT) | (localX & SECTION_MASK);
        final int absoluteZ = (sectionZ << SECTION_SHIFT) | (localZ & SECTION_MASK);

        return absoluteX + "." + absoluteZ + FILE_EXTERNAL_EXTENSION;
    }

    public static String getExternalTempFileName(final int sectionX, final int sectionZ,
                                                  final int localX, final int localZ) {
        final int absoluteX = (sectionX << SECTION_SHIFT) | (localX & SECTION_MASK);
        final int absoluteZ = (sectionZ << SECTION_SHIFT) | (localZ & SECTION_MASK);

        return absoluteX + "." + absoluteZ + FILE_EXTERNAL_TMP_EXTENSION;
    }

    public static final int SECTOR_SHIFT = 9;
    public static final int SECTOR_SIZE = 1 << SECTOR_SHIFT;

    public static final int SECTION_SHIFT = 5;
    public static final int SECTION_SIZE = 1 << SECTION_SHIFT;
    public static final int SECTION_MASK = SECTION_SIZE - 1;

    // General assumptions: Type header offsets are at least one sector in size

    /*
     * Note: All multibyte values are Big-Endian
     * Extension: cisf (Coordinate-Indexed Sector File)
     * External extension: cisfe (Coordinate-Indexed Sector File External)
     * Format:
     * Stores SECTION_SIZE^2 data objects mapped by a coordinate pair (x, z), where x, z are from [0, SECTION_SIZE)
     * All bytes are written in network order, or big-endian.
     *
     * First SECTOR_SIZE bytes: An int array indicating the start sector for a type header. The index is determined
     * externally by the program using CoordinateIndexedSectorFile.
     *
     * Type header: int array of SECTION_SIZE^2 elements.
     *
     * Type header array element:
     * Lower 11 bits -> sector length of data (max length: ~1MB)
     * Upper 21 bits -> sector offset in file (max file size: ~1GB)
     *
     * Data header format:
     * See bottom of class
     */

    private static final int SECTOR_LENGTH_BITS = 11;
    private static final int SECTOR_OFFSET_BITS = 21;

    private static final int MAX_NORMAL_SECTOR_OFFSET = (1 << SECTOR_OFFSET_BITS) - 2; // inclusive
    private static final int MAX_NORMAL_SECTOR_LENGTH = (1 << SECTOR_LENGTH_BITS) - 1;

    private static final int MAX_INTERNAL_ALLOCATION_BYTES = SECTOR_SIZE * (1 << SECTOR_LENGTH_BITS);

    private static final int HEADER_SECTOR = 0;
    private static final int HEADER_SECTORS = 1;
    private static final int HEADER_SIZE = (HEADER_SECTORS * SECTOR_SIZE) / INT_SIZE; // total number of header values (header sectors are fixed at 1)

    private static final int TYPE_HEADER_OFFSET_COUNT = SECTION_SIZE * SECTION_SIZE; // total number of offsets per type header
    private static final int TYPE_HEADER_SECTORS = (TYPE_HEADER_OFFSET_COUNT * INT_SIZE) / SECTOR_SIZE; // total number of sectors used per type header

    // header location is just raw sector number
    // so, we point to the header itself to indicate absence
    private static final int ABSENT_HEADER_LOCATION = HEADER_SECTOR;

    private static int makeLocation(final int sectorOffset, final int sectorLength) {
        return (sectorOffset << SECTOR_LENGTH_BITS) | (sectorLength & ((1 << SECTOR_LENGTH_BITS) - 1));
    }

    // point to header sector when absent, as we know that sector is allocated and will not conflict with any real allocation
    private static final int ABSENT_LOCATION              = makeLocation(HEADER_SECTOR, 0);
    // point to outside the maximum allocatable range for external allocations, which will not conflict with any other
    // data allocation (although, it may conflict with a type header allocation)
    private static final int EXTERNAL_ALLOCATION_LOCATION = makeLocation(MAX_NORMAL_SECTOR_OFFSET + 1, 0);

    private static int getLocationOffset(final int location) {
        return location >>> SECTOR_LENGTH_BITS;
    }

    private static int getLocationLength(final int location) {
        return location & ((1 << SECTOR_LENGTH_BITS) - 1);
    }

    private static int getIndex(final int localX, final int localZ) {
        return (localX & SECTION_MASK) | ((localZ & SECTION_MASK) << SECTION_SHIFT);
    }

    private static int getLocalX(final int index) {
        return index & SECTION_MASK;
    }

    private static int getLocalZ(final int index) {
        return (index >>> SECTION_SHIFT) & SECTION_MASK;
    }

    public final File file;
    public final int sectionX;
    public final int sectionZ;
    private final FileChannel channel;
    private final boolean sync;
    private final boolean readOnly;
    private final SectorAllocator sectorAllocator = newSectorAllocator();
    private final SectorFileCompressionType compressionType;

    private static final class TypeHeader {
        public final int headerDataOffset;
        public final int[] headerData;

        private TypeHeader(final int headerDataOffset, final int[] headerData) {
            this.headerDataOffset = headerDataOffset;
            this.headerData = headerData;
        }
    }

    private final Int2ObjectLinkedOpenHashMap<TypeHeader> typeHeaders = new Int2ObjectLinkedOpenHashMap<>();
    private final Int2ObjectMap<String> typeTranslationTable;

    private void checkReadOnlyHeader(final int type) {
        // we want to error when a type is used which is not mapped, but we can only store into typeHeaders in write mode
        // as sometimes we may need to create absent type headers
        if (this.typeTranslationTable.get(type) == null) {
            throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    public CoordinateIndexedSectorFile(final File file, final int sectionX, final int sectionZ, final boolean sync,
                                       final SectorFileCompressionType defaultCompressionType,
                                       final BufferChoices unscopedBufferChoices, final Int2ObjectMap<String> typeTranslationTable,
                                       final boolean readOnly) throws IOException {
        if (readOnly && sync) {
            throw new IllegalArgumentException("Cannot set read-only and sync");
        }
        this.file = file;
        this.sectionX = sectionX;
        this.sectionZ = sectionZ;
        if (readOnly) {
            this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } else {
            if (sync) {
                this.channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC);
            } else {
                this.channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            }
        }
        this.sync = sync;
        this.readOnly = readOnly;
        this.typeTranslationTable = typeTranslationTable;
        this.compressionType = defaultCompressionType;

        try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
            final ByteBuffer ioBuffer = scopedBufferChoices.t16k().acquireDirectBuffer();

            if (this.channel.size() != 0L) {
                try (final BufferChoices headerBuffers = scopedBufferChoices.scope()) {
                    this.readHeader(ioBuffer, headerBuffers.t16k().acquireDirectBuffer());
                }
            }

            boolean modifiedHeader = false;

            // make sure we have the type headers required allocated
            for (final IntIterator iterator = typeTranslationTable.keySet().iterator(); iterator.hasNext(); ) {
                final int type = iterator.nextInt();
                TypeHeader headerData = this.typeHeaders.get(type);
                if (headerData != null || readOnly) {
                    // allocated or unable to allocate
                    continue;
                }

                modifiedHeader = true;

                // need to allocate space for new type header
                final int offset = this.sectorAllocator.allocate(TYPE_HEADER_SECTORS, false); // in sectors
                if (offset <= 0) {
                    throw new IllegalStateException("Cannot allocate space for header " + this.debugType(type) + ":" + offset);
                }

                headerData = new TypeHeader(offset, new int[TYPE_HEADER_OFFSET_COUNT]);

                ioBuffer.limit(TYPE_HEADER_SECTORS * SECTOR_SIZE);
                ioBuffer.position(0);
                ioBuffer.asIntBuffer().put(0, headerData.headerData);
                this.write(ioBuffer, (long)offset << SECTOR_SHIFT);

                this.typeHeaders.put(type, headerData);
            }

            // modified the header, so write it back
            if (modifiedHeader) {
                this.writeHeader(ioBuffer);
            }
        }
    }

    private String debugType(final int type) {
        final String name = this.typeTranslationTable.get(type);
        return "{id=" + type + ",name=" + (name == null ? "unknown" : name) + "}";
    }

    private static SectorAllocator newSectorAllocator() {
        final SectorAllocator newSectorAllocation = new SectorAllocator(MAX_NORMAL_SECTOR_OFFSET, MAX_NORMAL_SECTOR_LENGTH);
        if (!newSectorAllocation.tryAllocateDirect(HEADER_SECTOR, HEADER_SECTORS, false)) {
            throw new IllegalStateException("Cannot allocate initial header");
        }
        return newSectorAllocation;
    }

    private void recalculateFile() {
        if (this.readOnly) {
            return;
        }
        LOGGER.error("An inconsistency has been detected in the headers for file '" + this.file.getAbsolutePath() + "', recalculating the headers", new Throwable());
        // The header was determined as incorrect, so we are going to rebuild it from the file
        final SectorAllocator newSectorAllocation = newSectorAllocator();

        // TODO
    }

    private String getAbsoluteCoordinate(final int index) {
        return this.getAbsoluteCoordinate(getLocalX(index), getLocalZ(index));
    }

    private String getAbsoluteCoordinate(final int localX, final int localZ) {
        return "(" + (localX | (this.sectionX << SECTION_SHIFT)) + "," + (localZ | (this.sectionZ << SECTION_SHIFT)) + ")";
    }

    private void write(final ByteBuffer buffer, long position) throws IOException {
        int len = buffer.remaining();
        while (len > 0) {
            final int written = this.channel.write(buffer, position);
            len -= written;
            position += (long)written;
        }
    }

    private void writeHeader(final ByteBuffer ioBuffer) throws IOException {
        ioBuffer.position(0);
        ioBuffer.limit(HEADER_SIZE * INT_SIZE);

        final IntBuffer ioIntBuffer = ioBuffer.asIntBuffer();
        // set default state
        for (int i = 0; i < HEADER_SIZE; ++i) {
            ioIntBuffer.put(i, ABSENT_HEADER_LOCATION);
        }

        for (final Iterator<Int2ObjectMap.Entry<TypeHeader>> iterator = this.typeHeaders.int2ObjectEntrySet().fastIterator(); iterator.hasNext();) {
            final Int2ObjectMap.Entry<TypeHeader> entry = iterator.next();

            final int type = entry.getIntKey();
            final TypeHeader header = entry.getValue();
            final int offset = header.headerDataOffset;

            ioIntBuffer.put(type, offset);
        }

        this.write(ioBuffer, (long)HEADER_SECTOR << SECTOR_SHIFT);
    }

    private void readHeader(final ByteBuffer ioBufferHeader, final ByteBuffer ioBufferTypeHeader) throws IOException {
        // reset sector allocations + headers for debug/testing
        this.sectorAllocator.copyAllocations(newSectorAllocator());
        this.typeHeaders.clear();

        ioBufferHeader.position(0);
        ioBufferHeader.limit(HEADER_SIZE * INT_SIZE);

        final long fileLengthSectors = (this.channel.size() + (SECTOR_SIZE - 1L)) >> SECTOR_SHIFT;

        int read = this.channel.read(ioBufferHeader, (long)HEADER_SECTOR << SECTOR_SHIFT);

        if (read != ioBufferHeader.limit()) {
            LOGGER.warn("File '" + this.file.getAbsolutePath() + "' has a truncated header");
            // File is truncated
            // All headers will initialise to 0
            return;
        }

        ioBufferHeader.position(0);

        final IntBuffer bufferHeaderInt = ioBufferHeader.asIntBuffer();

        // delay recalculation so that the logs contain all errors found
        boolean needsRecalculation = false;

        // try to allocate space for type headers
        for (int i = 0; i < HEADER_SIZE; ++i) {
            final int typeHeaderOffset = bufferHeaderInt.get(i);
            if (typeHeaderOffset == ABSENT_HEADER_LOCATION) {
                // no data
                continue;
            }
            // note: only the type headers can bypass the max limit, as the max limit is determined by SECTOR_OFFSET_BITS
            //       but the type offset is full 31 bits
            if (typeHeaderOffset < 0 || !this.sectorAllocator.tryAllocateDirect(typeHeaderOffset, TYPE_HEADER_SECTORS, true)) {
                LOGGER.error("File '" + this.file.getAbsolutePath() + "' has bad or overlapping offset for type " + this.debugType(i) + ": " + typeHeaderOffset);
                needsRecalculation = true;
                continue;
            }

            final String typeName = this.typeTranslationTable.get(i);
            if (typeName == null) {
                LOGGER.warn("File '" + this.file.getAbsolutePath() + "' has an unknown type header: " + i);
            }

            // parse header
            ioBufferTypeHeader.position(0);
            ioBufferTypeHeader.limit(TYPE_HEADER_SECTORS * SECTOR_SIZE);
            read = this.channel.read(ioBufferTypeHeader, (long)typeHeaderOffset << SECTOR_SHIFT);

            if (read != ioBufferTypeHeader.limit()) {
                LOGGER.error("File '" + this.file.getAbsolutePath() + "' has type header " + this.debugType(i) + " pointing to outside of file: " + typeHeaderOffset);
                needsRecalculation = true;
                continue;
            }

            final int[] header = new int[TYPE_HEADER_OFFSET_COUNT];
            ioBufferTypeHeader.flip().asIntBuffer().get(0, header, 0, header.length);

            // here, we now will try to allocate space for the data in the type header
            // we need to do it even if we don't know what type we're dealing with
            for (int k = 0; k < header.length; ++k) {
                final int location = header[k];
                if (location == ABSENT_LOCATION || location == EXTERNAL_ALLOCATION_LOCATION) {
                    // no data or it is on the external file
                    continue;
                }

                final int locationOffset = getLocationOffset(location);
                final int locationLength = getLocationLength(location);

                if (locationOffset < 0 || locationLength <= 0 || (locationOffset > fileLengthSectors || (locationOffset + locationLength) > fileLengthSectors)
                    || !this.sectorAllocator.tryAllocateDirect(locationOffset, locationLength, false)) {
                    LOGGER.error("File '" + this.file.getAbsolutePath() + "' has bad sector allocation for type " + this.debugType(i) + " located at " + this.getAbsoluteCoordinate(k));
                    needsRecalculation = true;
                    continue;
                }
            }

            this.typeHeaders.put(i, new TypeHeader(typeHeaderOffset, header));
        }

        if (needsRecalculation) {
            this.recalculateFile();
            return;
        }

        return;
    }

    private void updateAndWriteTypeHeader(final ByteBuffer ioBuffer, final int type, final int index, final int to) throws IOException {
        final TypeHeader headerData = this.typeHeaders.get(type);
        if (headerData == null) {
            throw new IllegalStateException("Unhandled type: " + type);
        }

        final int[] header = headerData.headerData;

        header[index] = to;

        ioBuffer.position(0);
        ioBuffer.limit(TYPE_HEADER_SECTORS * SECTOR_SIZE);

        ioBuffer.asIntBuffer().put(0, header, 0, header.length);

        this.write(ioBuffer, (long)headerData.headerDataOffset << SECTOR_SHIFT);
    }

    private void deleteExternalFile(final int localX, final int localZ) throws IOException {
        // use deleteIfExists for error reporting
        Files.deleteIfExists(this.getExternalFile(localX, localZ).toPath());
    }

    private File getExternalFile(final int localX, final int localZ) {
        return new File(this.file.getParentFile(), getExternalFileName(this.sectionX, this.sectionZ, localX, localZ));
    }

    private File getExternalTempFile(final int localX, final int localZ) throws IOException {
        return new File(this.file.getParentFile(), getExternalTempFileName(this.sectionX, this.sectionZ, localX, localZ));
    }

    public static Long computeExternalHash(final BufferChoices unscopedBufferChoices, final File externalFile) throws IOException {
        if (!externalFile.isFile() || externalFile.length() < (long)DataHeader.DATA_HEADER_LENGTH) {
            return null;
        }

        try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope();
             final StreamingXXHash64 streamingXXHash64 = XXHASH_JAVA_FACTORY.newStreamingHash64(XXHASH_SEED);
             final InputStream fileInput = Files.newInputStream(externalFile.toPath(), StandardOpenOption.READ)) {
            final byte[] bytes = scopedBufferChoices.t16k().acquireJavaBuffer();

            // first, skip header
            try {
                fileInput.skipNBytes((long)DataHeader.DATA_HEADER_LENGTH);
            } catch (final EOFException ex) {
                return null;
            }

            int r;
            while ((r = fileInput.read(bytes)) >= 0) {
                streamingXXHash64.update(bytes, 0, r);
            }

            return streamingXXHash64.getValue();
        }
    }

    public static final int READ_FLAG_CHECK_HEADER_HASH          = 1 << 0;
    public static final int READ_FLAG_CHECK_INTERNAL_DATA_HASH   = 1 << 1;
    public static final int READ_FLAG_CHECK_EXTERNAL_DATA_HASH   = 1 << 2;

    // do not check external data hash, there is not much we can do if it is actually bad
    public static final int RECOMMENDED_READ_FLAGS = READ_FLAG_CHECK_HEADER_HASH | READ_FLAG_CHECK_INTERNAL_DATA_HASH;
    // checks external hash additionally, which requires a separate full file read
    public static final int FULL_VALIDATION_FLAGS = READ_FLAG_CHECK_HEADER_HASH | READ_FLAG_CHECK_INTERNAL_DATA_HASH | READ_FLAG_CHECK_EXTERNAL_DATA_HASH;

    public boolean hasData(final int localX, final int localZ, final int type) {
        if (localX < 0 || localX > SECTION_MASK) {
            throw new IllegalArgumentException("X-coordinate out of range");
        }
        if (localZ < 0 || localZ > SECTION_MASK) {
            throw new IllegalArgumentException("Z-coordinate out of range");
        }

        final TypeHeader typeHeader = this.typeHeaders.get(type);

        if (typeHeader == null) {
            this.checkReadOnlyHeader(type);
            return false;
        }

        final int index = getIndex(localX, localZ);
        final int location = typeHeader.headerData[index];

        return location != ABSENT_LOCATION;
    }

    public DataInputStream read(final BufferChoices scopedBufferChoices, final int localX, final int localZ, final int type, final int readFlags) throws IOException {
        return this.read(scopedBufferChoices, scopedBufferChoices.t1m().acquireDirectBuffer(), localX, localZ, type, readFlags);
    }

    private DataInputStream tryRecalculate(final String reason, final BufferChoices scopedBufferChoices, final ByteBuffer buffer, final int localX, final int localZ, final int type, final int readFlags) throws IOException {
        LOGGER.error("File '" + this.file.getAbsolutePath() + "' has error at data for type " + this.debugType(type) + " located at " + this.getAbsoluteCoordinate(getIndex(localX, localZ)) + ": " + reason);
        // attribute error to bad header data, which we can re-calculate and re-try
        if (this.readOnly) {
            // cannot re-calculate, so we can only return null
            return null;
        }
        this.recalculateFile();
        // recalculate ensures valid data
        return this.read(scopedBufferChoices, buffer, localX, localZ, type, readFlags);
    }

    private DataInputStream read(final BufferChoices scopedBufferChoices, final ByteBuffer buffer, final int localX, final int localZ, final int type, final int readFlags) throws IOException {
        if (localX < 0 || localX > SECTION_MASK) {
            throw new IllegalArgumentException("X-coordinate out of range");
        }
        if (localZ < 0 || localZ > SECTION_MASK) {
            throw new IllegalArgumentException("Z-coordinate out of range");
        }

        if (buffer.capacity() < MAX_INTERNAL_ALLOCATION_BYTES) {
            throw new IllegalArgumentException("Buffer size must be at least " + MAX_INTERNAL_ALLOCATION_BYTES + " bytes");
        }

        buffer.limit(buffer.capacity());
        buffer.position(0);

        final TypeHeader typeHeader = this.typeHeaders.get(type);

        if (typeHeader == null) {
            this.checkReadOnlyHeader(type);
            return null;
        }

        final int index = getIndex(localX, localZ);

        final int location = typeHeader.headerData[index];

        if (location == ABSENT_LOCATION) {
            return null;
        }

        final boolean external = location == EXTERNAL_ALLOCATION_LOCATION;

        final ByteBufferInputStream rawIn;
        final File externalFile;
        if (external) {
            externalFile = this.getExternalFile(localX, localZ);

            rawIn = new BufferedFileChannelInputStream(buffer, externalFile);
        } else {
            externalFile = null;

            final int offset = getLocationOffset(location);
            final int length = getLocationLength(location);

            buffer.limit(length << SECTOR_SHIFT);
            this.channel.read(buffer, (long)offset << SECTOR_SHIFT);
            buffer.flip();

            rawIn = new ByteBufferInputStream(buffer);
        }

        final DataHeader dataHeader = DataHeader.read(rawIn);

        if (dataHeader == null) {
            return this.tryRecalculate("truncated " + (external ? "external" : "internal") + " data header", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
        }

        if ((readFlags & READ_FLAG_CHECK_HEADER_HASH) != 0) {
            if (!DataHeader.validate(XXHASH64, buffer, 0)) {
                return this.tryRecalculate("mismatch of " + (external ? "external" : "internal") + " data header hash", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
            }
        }

        if (dataHeader.typeId != type) {
            return this.tryRecalculate("mismatch of expected type and data header type", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
        }

        if (((int)dataHeader.index & 0xFFFF) != index) {
            return this.tryRecalculate("mismatch of expected coordinates and data header coordinates", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
        }

        // this is accurate for our implementations
        final int bytesAvailable = rawIn.available();

        if (external) {
            // for external files, the remaining size should exactly match the compressed size
            if (bytesAvailable != dataHeader.compressedSize) {
                return this.tryRecalculate("mismatch of external size and data header size", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
            }
        } else {
            // for non-external files, the remaining size should be >= compressed size AND the
            // compressed size should be on the same sector
            if (bytesAvailable < dataHeader.compressedSize || ((bytesAvailable + DataHeader.DATA_HEADER_LENGTH + (SECTOR_SIZE - 1)) >>> SECTOR_SHIFT) != ((dataHeader.compressedSize + DataHeader.DATA_HEADER_LENGTH + (SECTOR_SIZE - 1)) >>> SECTOR_SHIFT)) {
                return this.tryRecalculate("mismatch of internal size and data header size", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
            }
            // adjust max buffer to prevent reading over
            buffer.limit(buffer.position() + dataHeader.compressedSize);
            if (rawIn.available() != dataHeader.compressedSize) {
                // should not be possible
                throw new IllegalStateException();
            }
        }

        final byte compressType = dataHeader.compressionType;
        final SectorFileCompressionType compressionType = SectorFileCompressionType.getById((int)compressType);
        if (compressionType == null) {
            LOGGER.error("File '" + this.file.getAbsolutePath() + "' has unrecognized compression type for data type " + this.debugType(type) + " located at " + this.getAbsoluteCoordinate(index));
            // recalculate will not clobber data types if the compression is unrecognized, so we can only return null here
            return null;
        }

        if (!external && (readFlags & READ_FLAG_CHECK_INTERNAL_DATA_HASH) != 0) {
            final long expectedHash = XXHASH64.hash(buffer, buffer.position(), dataHeader.compressedSize, XXHASH_SEED);
            if (expectedHash != dataHeader.xxhash64Data) {
                return this.tryRecalculate("mismatch of internal data hash and data header hash", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
            }
        } else if (external && (readFlags & READ_FLAG_CHECK_EXTERNAL_DATA_HASH) != 0) {
            final Long externalHash = computeExternalHash(scopedBufferChoices, externalFile);
            if (externalHash == null || externalHash.longValue() != dataHeader.xxhash64Data) {
                return this.tryRecalculate("mismatch of external data hash and data header hash", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
            }
        }

        return new DataInputStream(compressionType.createInput(scopedBufferChoices, rawIn));
    }

    public boolean delete(final BufferChoices unscopedBufferChoices, final int localX, final int localZ, final int type) throws IOException {
        if (localX < 0 || localX > SECTION_MASK) {
            throw new IllegalArgumentException("X-coordinate out of range");
        }
        if (localZ < 0 || localZ > SECTION_MASK) {
            throw new IllegalArgumentException("Z-coordinate out of range");
        }

        if (this.readOnly) {
            throw new UnsupportedOperationException("Sector file is read-only");
        }

        final TypeHeader typeHeader = this.typeHeaders.get(type);

        if (typeHeader == null) {
            this.checkReadOnlyHeader(type);
            return false;
        }

        final int index = getIndex(localX, localZ);
        final int location = typeHeader.headerData[index];

        if (location == ABSENT_LOCATION) {
            return false;
        }

        // whether the location is external or internal, we delete from the type header before attempting anything else
        try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
            this.updateAndWriteTypeHeader(scopedBufferChoices.t16k().acquireDirectBuffer(), type, index, ABSENT_LOCATION);
        }

        // only proceed to try to delete sector allocation or external file if we succeed in deleting the type header entry

        if (location == EXTERNAL_ALLOCATION_LOCATION) {
            // only try to delete if the header write may succeed
            this.deleteExternalFile(localX, localZ);

            // no sector allocation to free

            return true;
        } else {
            final int offset = getLocationOffset(location);
            final int length = getLocationLength(location);

            this.sectorAllocator.freeAllocation(offset, length);

            return true;
        }
    }

    // performs a sync as if the sync flag is used for creating the sector file
    public static final int WRITE_FLAG_SYNC         = 1 << 0;

    public static record SectorFileOutput(
            /* Must run save (before close()) to cause the data to be written to the file, close() will not do this */
            CoordinateIndexedSectorFileOutput rawOutput,
            /* Close is required to run on the outputstream to free resources, but will not commit the data */
            DataOutputStream outputStream
    ) {}

    public SectorFileOutput write(final BufferChoices scopedBufferChoices, final int localX, final int localZ, final int type,
                                  final SectorFileCompressionType forceCompressionType, final int writeFlags) throws IOException {
        if (this.readOnly) {
            throw new UnsupportedOperationException("Sector file is read-only");
        }

        if (this.typeHeaders.get(type) == null) {
            throw new IllegalArgumentException("Unknown type " + type);
        }

        final SectorFileCompressionType useCompressionType = forceCompressionType == null ? this.compressionType : forceCompressionType;

        final CoordinateIndexedSectorFileOutput output = new CoordinateIndexedSectorFileOutput(
                scopedBufferChoices, localX, localZ, type, useCompressionType, writeFlags
        );
        final OutputStream compressedOut = useCompressionType.createOutput(scopedBufferChoices, output);

        return new SectorFileOutput(output, new DataOutputStream(compressedOut));
    }

    // expect buffer to be flipped (pos = 0, lim = written data) AND for the buffer to have the first DATA_HEADER_LENGTH
    // allocated to the header
    private void writeInternal(final BufferChoices unscopedBufferChoices, final ByteBuffer buffer, final int localX,
                               final int localZ, final int type, final long dataHash,
                               final SectorFileCompressionType compressionType, final int writeFlags) throws IOException {
        final int totalSize = buffer.limit();
        final int compressedSize = totalSize - DataHeader.DATA_HEADER_LENGTH;

        final int index = getIndex(localX, localZ);

        DataHeader.storeHeader(
            buffer.duplicate(), XXHASH64, dataHash, System.currentTimeMillis(), compressedSize,
            (short)index, (byte)type, (byte)compressionType.getId()
        );

        final int requiredSectors = (totalSize + (SECTOR_SIZE - 1)) >> SECTOR_SHIFT;

        if (requiredSectors > MAX_NORMAL_SECTOR_LENGTH) {
            throw new IllegalArgumentException("Provided data is too large for internal write");
        }

        // allocate new space, write to it, and only after that is successful free the old allocation if it exists

        final int sectorToStore = this.sectorAllocator.allocate(requiredSectors, true);
        if (sectorToStore < 0) {
            // TODO need to store externally, ran out of space in this file
            throw new UnsupportedOperationException();
        }

        // write data to allocated space
        this.write(buffer, (long)sectorToStore << SECTOR_SHIFT);

        final int prevLocation = this.typeHeaders.get(type).headerData[index];

        // update header on disk
        final int newLocation = makeLocation(sectorToStore, requiredSectors);

        try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
            this.updateAndWriteTypeHeader(scopedBufferChoices.t16k().acquireDirectBuffer(), type, index, newLocation);
        }

        // force disk updates if required
        if ((writeFlags & WRITE_FLAG_SYNC) != 0) {
            this.channel.force(true);
        }

        // finally, now we are certain there are no references to the prev location, we can de-allocate
        if (prevLocation != ABSENT_LOCATION) {
            if (prevLocation == EXTERNAL_ALLOCATION_LOCATION) {
                // de-allocation is done by deleting external file
                this.deleteExternalFile(localX, localZ);
            } else {
                // just need to free the sector allocation
                this.sectorAllocator.freeAllocation(getLocationOffset(prevLocation), getLocationLength(prevLocation));
            }
        } // else: nothing to free
    }

    public final class CoordinateIndexedSectorFileOutput extends ByteBufferOutputStream {
        private final BufferChoices scopedBufferChoices;

        private FileChannel externalFile;
        private StreamingXXHash64 externalHash;

        private final int localX;
        private final int localZ;
        private final int type;
        private final SectorFileCompressionType compressionType;
        private final int writeFlags;

        private CoordinateIndexedSectorFileOutput(final BufferChoices scopedBufferChoices,
                                                  final int localX, final int localZ, final int type,
                                                  final SectorFileCompressionType compressionType,
                                                  final int writeFlags) {
            super(scopedBufferChoices.t1m().acquireDirectBuffer());
            // we use a lower limit than capacity to force flush() to be invoked before
            // the maximum internal size
            this.buffer.limit((MAX_NORMAL_SECTOR_LENGTH << SECTOR_SHIFT) | (SECTOR_SIZE - 1));
            // make space for the header
            this.buffer.position(DataHeader.DATA_HEADER_LENGTH);

            this.scopedBufferChoices = scopedBufferChoices;

            this.localX = localX;
            this.localZ = localZ;
            this.type = type;
            this.compressionType = compressionType;
            this.writeFlags = writeFlags;
        }

        @Override
        protected ByteBuffer flush(final ByteBuffer current) throws IOException {
            if (current.hasRemaining()) {
                return current;
            }
            // TODO oversized support
            throw new UnsupportedOperationException();
        }

        public void save() throws IOException {
            if (this.externalFile == null) {
                // avoid clobbering buffer positions/limits
                final ByteBuffer buffer = this.buffer.duplicate();

                buffer.flip();

                final long dataHash = XXHASH64.hash(
                        this.buffer, DataHeader.DATA_HEADER_LENGTH, buffer.remaining() - DataHeader.DATA_HEADER_LENGTH,
                        XXHASH_SEED
                );

                CoordinateIndexedSectorFile.this.writeInternal(
                        this.scopedBufferChoices, buffer, this.localX, this.localZ,
                        this.type, dataHash, this.compressionType, this.writeFlags
                );
            } else {
                // TODO oversized support
                throw new UnsupportedOperationException();
            }
        }

        public void freeResources() throws IOException {

        }

        @Override
        public void close() throws IOException {
            try {
                super.flush();
                this.save();
            } finally {
                try {
                    super.close();
                } finally {
                    this.freeResources();
                }
            }
        }
    }

    public void flush() throws IOException {
        if (!this.channel.isOpen()) {
            return;
        }
        if (!this.readOnly) {
            if (this.sync) {
                this.channel.force(true);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!this.channel.isOpen()) {
            return;
        }

        try {
            if (!this.readOnly) {
                final int maxSector = this.sectorAllocator.getLastAllocatedBlock();
                if (maxSector >= 0) {
                    final long maxLength = ((long)maxSector << SECTOR_SHIFT) + (long)SECTOR_SIZE; // maxSector is inclusive
                    this.channel.truncate(maxLength);
                }
            }
        } finally {
            try {
                this.flush();
            } finally {
                this.channel.close();
            }
        }
    }

    private static record DataHeader(
            long xxhash64Header,
            long xxhash64Data,
            long timeWritten,
            int compressedSize,
            short index,
            byte typeId,
            byte compressionType
    ) {
        /*
         * First 8 bytes: XXHash64 of the header data
         * Next 8 bytes: XXHash64 of data
         * Next 8 bytes: currentTimeMillis value
         * Next 4 bytes: compressed data size
         * Next 2 bytes: encoded index of the location
         * Next 1 byte: data type's id
         * Next 1 byte: compression type
         */

        public static void storeHeader(final ByteBuffer buffer, final XXHash64 xxHash64,
                                       final long dataHash, final long timeWritten,
                                       final int compressedSize, final short index, final byte typeId,
                                       final byte compressionType) {
            final int pos = buffer.position();

            buffer.putLong(0L); // placeholder for header hash
            buffer.putLong(dataHash);
            buffer.putLong(timeWritten);
            buffer.putInt(compressedSize);
            buffer.putShort(index);
            buffer.put(typeId);
            buffer.put(compressionType);

            // replace placeholder for header hash with real hash
            buffer.putLong(pos, computeHash(xxHash64, buffer, pos));
        }

        private static final int DATA_HEADER_LENGTH = LONG_SIZE + LONG_SIZE + LONG_SIZE + INT_SIZE + SHORT_SIZE + BYTE_SIZE + BYTE_SIZE;

        public static DataHeader read(final ByteBuffer buffer) {
            if (buffer.remaining() < DATA_HEADER_LENGTH) {
                return null;
            }

            return new DataHeader(
                    buffer.getLong(), buffer.getLong(), buffer.getLong(),
                    buffer.getInt(), buffer.getShort(), buffer.get(), buffer.get()
            );
        }

        public static DataHeader read(final ByteBufferInputStream input) throws IOException {
            final ByteBuffer buffer = ByteBuffer.allocate(DATA_HEADER_LENGTH);

            // read = 0 when buffer is full
            while (input.read(buffer) > 0);

            buffer.flip();
            return read(buffer);
        }

        public static long computeHash(final XXHash64 xxHash64, final ByteBuffer header, final int offset) {
            return xxHash64.hash(header, offset + LONG_SIZE, DATA_HEADER_LENGTH - LONG_SIZE, XXHASH_SEED);
        }

        public static boolean validate(final XXHash64 xxHash64, final ByteBuffer header, final int offset) {
            final long expectedSeed = header.getLong(offset);
            final long computedSeed = computeHash(xxHash64, header, offset);

            return expectedSeed == computedSeed;
        }
    }

    public static final class SectorAllocator {

        // smallest size first, then by lowest position in file
        private final LongRBTreeSet freeBlocksBySize = new LongRBTreeSet((LongComparator)(final long a, final long b) -> {
            final int sizeCompare = Integer.compare(getFreeBlockLength(a), getFreeBlockLength(b));
            if (sizeCompare != 0) {
                return sizeCompare;
            }

            return Integer.compare(getFreeBlockStart(a), getFreeBlockStart(b));
        });

        private final LongRBTreeSet freeBlocksByOffset = new LongRBTreeSet((LongComparator)(final long a, final long b) -> {
            return Integer.compare(getFreeBlockStart(a), getFreeBlockStart(b));
        });

        private final int maxOffset; // inclusive
        private final int maxLength; // inclusive

        private static final int MAX_ALLOCATION = (Integer.MAX_VALUE >>> 1) + 1;
        private static final int MAX_LENGTH = (Integer.MAX_VALUE >>> 1) + 1;

        public SectorAllocator(final int maxOffset, final int maxLength) {
            this.maxOffset = maxOffset;
            this.maxLength = maxLength;

            this.reset();
        }

        public void reset() {
            this.freeBlocksBySize.clear();
            this.freeBlocksByOffset.clear();

            final long infiniteAllocation = makeFreeBlock(0, MAX_ALLOCATION);
            this.freeBlocksBySize.add(infiniteAllocation);
            this.freeBlocksByOffset.add(infiniteAllocation);
        }

        public void copyAllocations(final SectorAllocator other) {
            this.freeBlocksBySize.clear();
            this.freeBlocksBySize.addAll(other.freeBlocksBySize);

            this.freeBlocksByOffset.clear();
            this.freeBlocksByOffset.addAll(other.freeBlocksByOffset);
        }

        public int getLastAllocatedBlock() {
            if (this.freeBlocksByOffset.isEmpty()) {
                // entire space is allocated
                return MAX_ALLOCATION - 1;
            }

            final long lastFreeBlock = this.freeBlocksByOffset.lastLong();
            final int lastFreeStart = getFreeBlockStart(lastFreeBlock);
            final int lastFreeEnd = lastFreeStart + getFreeBlockLength(lastFreeBlock) - 1;

            if (lastFreeEnd == (MAX_ALLOCATION - 1)) {
                // no allocations past this block, so the end must be before this block
                // note: if lastFreeStart == 0, then we return - 1 which indicates no block has been allocated
                return lastFreeStart - 1;
            }
            return MAX_ALLOCATION - 1;
        }

        private static long makeFreeBlock(final int start, final int length) {
            return ((start & 0xFFFFFFFFL) | ((long)length << 32));
        }

        private static int getFreeBlockStart(final long freeBlock) {
            return (int)freeBlock;
        }

        private static int getFreeBlockLength(final long freeBlock) {
            return (int)(freeBlock >>> 32);
        }

        private void splitBlock(final long fromBlock, final int allocStart, final int allocEnd) {
            // allocEnd is inclusive

            // required to remove before adding again in case the split block's offset and/or length is the same
            this.freeBlocksByOffset.remove(fromBlock);
            this.freeBlocksBySize.remove(fromBlock);

            final int fromStart = getFreeBlockStart(fromBlock);
            final int fromEnd = getFreeBlockStart(fromBlock) + getFreeBlockLength(fromBlock) - 1;

            if (fromStart != allocStart) {
                // need to allocate free block to the left of the allocation
                if (allocStart < fromStart) {
                    throw new IllegalStateException();
                }
                final long leftBlock = makeFreeBlock(fromStart, allocStart - fromStart);
                this.freeBlocksByOffset.add(leftBlock);
                this.freeBlocksBySize.add(leftBlock);
            }

            if (fromEnd != allocEnd) {
                // need to allocate free block to the right of the allocation
                if (allocEnd > fromEnd) {
                    throw new IllegalStateException();
                }
                // fromEnd - allocEnd = (fromEnd + 1) - (allocEnd + 1)
                final long rightBlock = makeFreeBlock(allocEnd + 1, fromEnd - allocEnd);
                this.freeBlocksByOffset.add(rightBlock);
                this.freeBlocksBySize.add(rightBlock);
            }
        }

        public boolean tryAllocateDirect(final int from, final int length, final boolean bypassMax) {
            if (from < 0) {
                throw new IllegalArgumentException("From must be >= 0");
            }
            if (length <= 0) {
                throw new IllegalArgumentException("Length must be > 0");
            }

            final int end = from + length - 1; // inclusive

            if (end < 0 || end >= MAX_ALLOCATION || length >= MAX_LENGTH) {
                return false;
            }

            if (!bypassMax && (from > this.maxOffset || length > this.maxLength || end > this.maxOffset)) {
                return false;
            }

            final LongBidirectionalIterator iterator = this.freeBlocksByOffset.iterator(makeFreeBlock(from, 0));
            // iterator.next > curr
            // iterator.prev <= curr

            if (!iterator.hasPrevious()) {
                // only free blocks starting at from+1, if any
                return false;
            }

            final long block = iterator.previousLong();
            final int blockStart = getFreeBlockStart(block);
            final int blockLength = getFreeBlockLength(block);
            final int blockEnd = blockStart + blockLength - 1; // inclusive

            if (from > blockEnd || end > blockEnd) {
                return false;
            }

            if (from < blockStart) {
                throw new IllegalStateException();
            }

            this.splitBlock(block, from, end);

            return true;
        }

        public void freeAllocation(final int from, final int length) {
            if (from < 0) {
                throw new IllegalArgumentException("From must be >= 0");
            }
            if (length <= 0) {
                throw new IllegalArgumentException("Length must be > 0");
            }

            final int end = from + length - 1;
            if (end < 0 || end >= MAX_ALLOCATION || length >= MAX_LENGTH) {
                throw new IllegalArgumentException("End sector must be in allocation range");
            }

            final LongBidirectionalIterator iterator = this.freeBlocksByOffset.iterator(makeFreeBlock(from, length));
            // iterator.next > curr
            // iterator.prev <= curr

            long prev = -1L;
            int prevStart = 0;
            int prevEnd = 0;

            long next = -1L;
            int nextStart = 0;
            int nextEnd = 0;

            if (iterator.hasPrevious()) {
                prev = iterator.previousLong();
                prevStart = getFreeBlockStart(prev);
                prevEnd = prevStart + getFreeBlockLength(prev) - 1;
                // advance back for next usage
                iterator.nextLong();
            }

            if (iterator.hasNext()) {
                next = iterator.nextLong();
                nextStart = getFreeBlockStart(next);
                nextEnd = nextStart + getFreeBlockLength(next) - 1;
            }

            // first, check that we are not trying to free area in another free block
            if (prev != -1L) {
                if (from <= prevEnd && end >= prevStart) {
                    throw new IllegalArgumentException("free call overlaps with already free block");
                }
            }

            if (next != -1L) {
                if (from <= nextEnd && end >= nextStart) {
                    throw new IllegalArgumentException("free call overlaps with already free block");
                }
            }

            // try to merge with left & right free blocks
            int adjustedStart = from;
            int adjustedEnd = end;
            if (prev != -1L && (prevEnd + 1) == from) {
                adjustedStart = prevStart;
                // delete merged block
                this.freeBlocksByOffset.remove(prev);
                this.freeBlocksBySize.remove(prev);
            }

            if (next != -1L && nextStart == (end + 1)) {
                adjustedEnd = nextEnd;
                // delete merged block
                this.freeBlocksByOffset.remove(next);
                this.freeBlocksBySize.remove(next);
            }

            final long block = makeFreeBlock(adjustedStart, adjustedEnd - adjustedStart + 1);
            // add possibly merged free block
            this.freeBlocksByOffset.add(block);
            this.freeBlocksBySize.add(block);
        }

        // returns -1 if the allocation cannot be done due to length/position limitations
        public int allocate(final int length, final boolean checkMaxOffset) {
            if (length <= 0) {
                throw new IllegalArgumentException("Length must be > 0");
            }
            if (length > this.maxLength) {
                return -1;
            }

            if (this.freeBlocksBySize.isEmpty()) {
                return -1;
            }

            final LongBidirectionalIterator iterator = this.freeBlocksBySize.iterator(makeFreeBlock(-1, length));
            // iterator.next > curr
            // iterator.prev <= curr

            // if we use start = -1, then no block retrieved is <= curr as offset < 0 is invalid. Then, the iterator next()
            // returns >= makeFreeBlock(0, length) where the comparison is first by length then sector offset.
            // Thus, we can just select next() as the block to split. This makes the allocation best-fit in that it selects
            // first the smallest block that can fit the allocation, and that the smallest block selected offset is
            // as close to 0 compared to the rest of the blocks at the same size

            final long block = iterator.nextLong();
            final int blockStart = getFreeBlockStart(block);

            final int allocStart = blockStart;
            final int allocEnd = blockStart + length - 1;

            if (allocStart < 0) {
                throw new IllegalStateException();
            }

            if (allocEnd < 0) {
                // overflow
                return -1;
            }

            // note: we do not need to worry about overflow in splitBlock because the free blocks are only allocated
            // in [0, MAX_ALLOCATION - 1]

            if (checkMaxOffset && (allocEnd > this.maxOffset)) {
                return -1;
            }

            this.splitBlock(block, allocStart, allocEnd);

            return blockStart;
        }
    }
}
