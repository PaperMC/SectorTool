package ca.spottedleaf.io.region;

import ca.spottedleaf.io.region.io.bytebuffer.BufferedFileChannelInputStream;
import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferInputStream;
import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferOutputStream;
import ca.spottedleaf.io.buffer.BufferChoices;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public final class SectorFile implements Closeable {

    private static final XXHashFactory XXHASH_FACTORY = XXHashFactory.fastestInstance();
    // Java instance is used for streaming hash instances, as streaming hash instances do not provide bytebuffer API
    // Native instances would use GetPrimitiveArrayCritical and prevent GC on G1
    private static final XXHashFactory XXHASH_JAVA_FACTORY = XXHashFactory.fastestJavaInstance();
    private static final XXHash64 XXHASH64 = XXHASH_FACTORY.hash64();
    // did not find a use to change this from default, but just in case
    private static final long XXHASH_SEED = 0L;

    private static final Logger LOGGER = LoggerFactory.getLogger(SectorFile.class);

    private static final int BYTE_SIZE   = Byte.BYTES;
    private static final int SHORT_SIZE  = Short.BYTES;
    private static final int INT_SIZE    = Integer.BYTES;
    private static final int LONG_SIZE   = Long.BYTES;
    private static final int FLOAT_SIZE  = Float.BYTES;
    private static final int DOUBLE_SIZE = Double.BYTES;

    public static final String FILE_EXTENSION              = ".mcsl";
    public static final String FILE_EXTERNAL_EXTENSION     = ".mcsle";
    public static final String FILE_EXTERNAL_TMP_EXTENSION = FILE_EXTERNAL_EXTENSION + ".tmp";

    public static String getFileName(final int sectionX, final int sectionZ) {
        return sectionX + "." + sectionZ + FILE_EXTENSION;
    }

    private static String getExternalBase(final int sectionX, final int sectionZ,
                                          final int localX, final int localZ,
                                          final int type) {
        final int absoluteX = (sectionX << SECTION_SHIFT) | (localX & SECTION_MASK);
        final int absoluteZ = (sectionZ << SECTION_SHIFT) | (localZ & SECTION_MASK);

        return absoluteX + "." + absoluteZ + "-" + type;
    }

    public static String getExternalFileName(final int sectionX, final int sectionZ,
                                             final int localX, final int localZ,
                                             final int type) {
        return getExternalBase(sectionX, sectionZ, localX, localZ, type) + FILE_EXTERNAL_EXTENSION;
    }

    public static String getExternalTempFileName(final int sectionX, final int sectionZ,
                                                  final int localX, final int localZ, final int type) {
        return getExternalBase(sectionX, sectionZ, localX, localZ, type) + FILE_EXTERNAL_TMP_EXTENSION;
    }

    public static final int SECTOR_SHIFT = 9;
    public static final int SECTOR_SIZE = 1 << SECTOR_SHIFT;

    public static final int SECTION_SHIFT = 5;
    public static final int SECTION_SIZE = 1 << SECTION_SHIFT;
    public static final int SECTION_MASK = SECTION_SIZE - 1;

    // General assumptions: Type header offsets are at least one sector in size

    /*
     * File Header:
     * First 8-bytes: XXHash64 of entire header data, excluding hash value
     * Next 42x8 bytes: XXHash64 values for each type header
     * Next 42x4 bytes: sector offsets of type headers
     */
    private static final int FILE_HEADER_SECTOR = 0;
    public static final int MAX_TYPES = 42;

    public static final class FileHeader {

        public static final int FILE_HEADER_SIZE_BYTES = LONG_SIZE + MAX_TYPES*(LONG_SIZE + INT_SIZE);
        public static final int FILE_HEADER_TOTAL_SECTORS = (FILE_HEADER_SIZE_BYTES + (SECTOR_SIZE - 1)) >> SECTOR_SHIFT;

        public final long[] xxHash64TypeHeader = new long[MAX_TYPES];
        public final int[] typeHeaderOffsets = new int[MAX_TYPES];

        public FileHeader() {
            if (ABSENT_HEADER_XXHASH64 != 0L || ABSENT_TYPE_HEADER_OFFSET != 0) {
                this.reset();
            }
        }

        public void reset() {
            Arrays.fill(this.xxHash64TypeHeader, ABSENT_HEADER_XXHASH64);
            Arrays.fill(this.typeHeaderOffsets, ABSENT_TYPE_HEADER_OFFSET);
        }

        public void write(final ByteBuffer buffer) {
            final int pos = buffer.position();

            // reserve XXHash64 space
            buffer.putLong(0L);

            buffer.asLongBuffer().put(0, this.xxHash64TypeHeader);
            buffer.position(buffer.position() + MAX_TYPES * LONG_SIZE);

            buffer.asIntBuffer().put(0, this.typeHeaderOffsets);
            buffer.position(buffer.position() + MAX_TYPES * INT_SIZE);

            final long hash = computeHash(buffer, pos);

            buffer.putLong(pos, hash);
        }

        public static void read(final ByteBuffer buffer, final FileHeader fileHeader) {
            buffer.duplicate().position(buffer.position() + LONG_SIZE).asLongBuffer().get(0, fileHeader.xxHash64TypeHeader);

            buffer.duplicate().position(buffer.position() + LONG_SIZE + LONG_SIZE * MAX_TYPES)
                .asIntBuffer().get(0, fileHeader.typeHeaderOffsets);

            buffer.position(buffer.position() + FILE_HEADER_SIZE_BYTES);
        }

        public static long computeHash(final ByteBuffer buffer, final int offset) {
            return XXHASH64.hash(buffer, offset + LONG_SIZE, FILE_HEADER_SIZE_BYTES - LONG_SIZE, XXHASH_SEED);
        }

        public static boolean validate(final ByteBuffer buffer, final int offset) {
            final long expected = buffer.getLong(offset);

            return expected == computeHash(buffer, offset);
        }

        public void copyFrom(final FileHeader src) {
            System.arraycopy(src.xxHash64TypeHeader, 0, this.xxHash64TypeHeader, 0, MAX_TYPES);
            System.arraycopy(src.typeHeaderOffsets, 0, this.typeHeaderOffsets, 0, MAX_TYPES);
        }
    }

    public static record DataHeader(
            long xxhash64Header,
            long xxhash64Data,
            long timeWritten,
            int compressedSize,
            short index,
            byte typeId,
            byte compressionType
    ) {

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

        public static final int DATA_HEADER_LENGTH = LONG_SIZE + LONG_SIZE + LONG_SIZE + INT_SIZE + SHORT_SIZE + BYTE_SIZE + BYTE_SIZE;

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

    private static final int SECTOR_LENGTH_BITS = 10;
    private static final int SECTOR_OFFSET_BITS = 22;
    static {
        if ((SECTOR_OFFSET_BITS + SECTOR_LENGTH_BITS) != 32) {
            throw new IllegalStateException();
        }
    }

    private static final int MAX_NORMAL_SECTOR_OFFSET = (1 << SECTOR_OFFSET_BITS) - 2; // inclusive
    private static final int MAX_NORMAL_SECTOR_LENGTH = (1 << SECTOR_LENGTH_BITS) - 1;

    private static final int MAX_INTERNAL_ALLOCATION_BYTES = SECTOR_SIZE * (1 << SECTOR_LENGTH_BITS);

    private static final int TYPE_HEADER_OFFSET_COUNT = SECTION_SIZE * SECTION_SIZE; // total number of offsets per type header
    private static final int TYPE_HEADER_SECTORS = (TYPE_HEADER_OFFSET_COUNT * INT_SIZE) / SECTOR_SIZE; // total number of sectors used per type header

    // header location is just raw sector number
    // so, we point to the header itself to indicate absence
    private static final int ABSENT_TYPE_HEADER_OFFSET = FILE_HEADER_SECTOR;
    private static final long ABSENT_HEADER_XXHASH64 = 0L;

    private static int makeLocation(final int sectorOffset, final int sectorLength) {
        return (sectorOffset << SECTOR_LENGTH_BITS) | (sectorLength & ((1 << SECTOR_LENGTH_BITS) - 1));
    }

    // point to file header sector when absent, as we know that sector is allocated and will not conflict with any real allocation
    private static final int ABSENT_LOCATION              = makeLocation(FILE_HEADER_SECTOR, 0);
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

        public static final int TYPE_HEADER_SIZE_BYTES = TYPE_HEADER_OFFSET_COUNT * INT_SIZE;

        public final int[] locations;

        private TypeHeader() {
            this.locations = new int[TYPE_HEADER_OFFSET_COUNT];
            if (ABSENT_LOCATION != 0) {
                this.reset();
            }
        }

        private TypeHeader(final int[] locations) {
            this.locations = locations;
            if (locations.length != TYPE_HEADER_OFFSET_COUNT) {
                throw new IllegalArgumentException();
            }
        }

        public void reset() {
            Arrays.fill(this.locations, ABSENT_LOCATION);
        }

        public static TypeHeader read(final ByteBuffer buffer) {
            final int[] locations = new int[TYPE_HEADER_OFFSET_COUNT];
            buffer.asIntBuffer().get(0, locations, 0, TYPE_HEADER_OFFSET_COUNT);

            return new TypeHeader(locations);
        }

        public void write(final ByteBuffer buffer) {
            buffer.asIntBuffer().put(0, this.locations);

            buffer.position(buffer.position() + TYPE_HEADER_SIZE_BYTES);
        }

        public static long computeHash(final ByteBuffer buffer, final int offset) {
            return XXHASH64.hash(buffer, offset, TYPE_HEADER_SIZE_BYTES, XXHASH_SEED);
        }
    }

    private final Int2ObjectLinkedOpenHashMap<TypeHeader> typeHeaders = new Int2ObjectLinkedOpenHashMap<>();
    private final Int2ObjectMap<String> typeTranslationTable;
    private final FileHeader fileHeader = new FileHeader();

    private void checkReadOnlyHeader(final int type) {
        // we want to error when a type is used which is not mapped, but we can only store into typeHeaders in write mode
        // as sometimes we may need to create absent type headers
        if (this.typeTranslationTable.get(type) == null) {
            throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    static {
        final int smallBufferSize = 16 * 1024; // 16kb
        if (FileHeader.FILE_HEADER_SIZE_BYTES > smallBufferSize) {
            throw new IllegalStateException("Cannot read file header using single small buffer");
        }
        if (TypeHeader.TYPE_HEADER_SIZE_BYTES > smallBufferSize) {
            throw new IllegalStateException("Cannot read type header using single small buffer");
        }
    }

    public static final int OPEN_FLAGS_READ_ONLY   = 1 << 0;
    public static final int OPEN_FLAGS_SYNC_WRITES = 1 << 1;

    public SectorFile(final File file, final int sectionX, final int sectionZ,
                      final SectorFileCompressionType defaultCompressionType,
                      final BufferChoices unscopedBufferChoices, final Int2ObjectMap<String> typeTranslationTable,
                      final int openFlags) throws IOException {
        final boolean readOnly = (openFlags & OPEN_FLAGS_READ_ONLY) != 0;
        final boolean sync = (openFlags & OPEN_FLAGS_SYNC_WRITES) != 0;

        if (readOnly & sync) {
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

        if (this.channel.size() != 0L) {
            this.readFileHeader(unscopedBufferChoices);
        }

        boolean modifiedFileHeader = false;

        try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
            final ByteBuffer ioBuffer = scopedBufferChoices.t16k().acquireDirectBuffer();

            // make sure we have the type headers required allocated
            for (final IntIterator iterator = typeTranslationTable.keySet().iterator(); iterator.hasNext(); ) {
                final int type = iterator.nextInt();

                if (type < 0 || type >= MAX_TYPES) {
                    throw new IllegalStateException("Type translation table contains illegal type: " + type);
                }

                final TypeHeader headerData = this.typeHeaders.get(type);
                if (headerData != null || readOnly) {
                    // allocated or unable to allocate
                    continue;
                }

                modifiedFileHeader = true;

                // need to allocate space for new type header
                final int offset = this.sectorAllocator.allocate(TYPE_HEADER_SECTORS, false); // in sectors
                if (offset <= 0) {
                    throw new IllegalStateException("Cannot allocate space for header " + this.debugType(type) + ":" + offset);
                }

                this.fileHeader.typeHeaderOffsets[type] = offset;
                // hash will be computed by writeTypeHeader
                this.typeHeaders.put(type, new TypeHeader());

                this.writeTypeHeader(ioBuffer, type, true, false);
            }

            // modified the file header, so write it back
            if (modifiedFileHeader) {
                this.writeFileHeader(ioBuffer);
            }
        }
    }

    public int forTestingAllocateSector(final int sectors) {
        return this.sectorAllocator.allocate(sectors, true);
    }

    private String debugType(final int type) {
        final String name = this.typeTranslationTable.get(type);
        return "{id=" + type + ",name=" + (name == null ? "unknown" : name) + "}";
    }

    private static SectorAllocator newSectorAllocator() {
        final SectorAllocator newSectorAllocation = new SectorAllocator(MAX_NORMAL_SECTOR_OFFSET, MAX_NORMAL_SECTOR_LENGTH);
        if (!newSectorAllocation.tryAllocateDirect(FILE_HEADER_SECTOR, FileHeader.FILE_HEADER_TOTAL_SECTORS, false)) {
            throw new IllegalStateException("Cannot allocate initial header");
        }
        return newSectorAllocation;
    }

    private void makeBackup(final File target) throws IOException {
        this.channel.force(true);
        Files.copy(this.file.toPath(), target.toPath(), StandardCopyOption.COPY_ATTRIBUTES);
    }

    public static final int RECALCULATE_FLAGS_NO_BACKUP = 1 << 0;
    public static final int RECALCULATE_FLAGS_NO_LOG    = 1 << 1;

    // returns whether any changes were made, useful for testing
    public boolean recalculateFile(final BufferChoices unscopedBufferChoices, final int flags) throws IOException {
        if (this.readOnly) {
            return false;
        }
        if ((flags & RECALCULATE_FLAGS_NO_LOG) == 0) {
            LOGGER.error("An inconsistency has been detected in the headers for file '" + this.file.getAbsolutePath() +
                "', recalculating the headers", new Throwable());
        }
        // The headers are determined as incorrect, so we are going to rebuild it from the file
        final SectorAllocator newSectorAllocation = newSectorAllocator();

        if ((flags & RECALCULATE_FLAGS_NO_BACKUP) == 0) {
            final File backup = new File(this.file.getParentFile(), this.file.getName() + "." + new Random().nextLong() + ".backup");
            if ((flags & RECALCULATE_FLAGS_NO_LOG) == 0) {
                LOGGER.info("Making backup of '" + this.file.getAbsolutePath() + "' to '" + backup.getAbsolutePath() + "'");
            }
            this.makeBackup(backup);
        }

        class TentativeTypeHeader {
            final TypeHeader typeHeader = new TypeHeader();
            final long[] timestamps = new long[TYPE_HEADER_OFFSET_COUNT];
        }

        final Int2ObjectLinkedOpenHashMap<TentativeTypeHeader> newTypeHeaders = new Int2ObjectLinkedOpenHashMap<>();

        // order of precedence of data found:
        // newest timestamp,
        // located internally,
        // located closest to start internally

        // force creation tentative type headers for required headers, as we will later replace the current ones
        for (final IntIterator iterator = this.typeTranslationTable.keySet().iterator(); iterator.hasNext();) {
            newTypeHeaders.put(iterator.nextInt(), new TentativeTypeHeader());
        }

        // search for internal data

        try (final BufferChoices scopedChoices = unscopedBufferChoices.scope()) {
            final ByteBuffer buffer = scopedChoices.t1m().acquireDirectBuffer();

            final long fileSectors = (this.channel.size() + (long)(SECTOR_SIZE - 1)) >>> SECTOR_SHIFT;
            for (long i = (long)(FILE_HEADER_SECTOR + FileHeader.FILE_HEADER_TOTAL_SECTORS); i <= Math.min(fileSectors, (long)MAX_NORMAL_SECTOR_OFFSET); ++i) {
                buffer.limit(DataHeader.DATA_HEADER_LENGTH);
                buffer.position(0);

                this.channel.read(buffer, i << SECTOR_SHIFT);

                if (buffer.hasRemaining()) {
                    // last sector, which is truncated
                    continue;
                }

                buffer.flip();

                if (!DataHeader.validate(XXHASH64, buffer, 0)) {
                    // no valid data allocated on this sector
                    continue;
                }

                final DataHeader dataHeader = DataHeader.read(buffer);
                // sector size = (compressed size + header size + SECTOR_SIZE-1) >> SECTOR_SHIFT
                final int maxCompressedSize = (MAX_NORMAL_SECTOR_LENGTH << SECTOR_SHIFT) - DataHeader.DATA_HEADER_LENGTH;

                if (dataHeader.compressedSize > maxCompressedSize || dataHeader.compressedSize < 0) {
                    // invalid size
                    continue;
                }

                final int typeId = (int)(dataHeader.typeId & 0xFF);
                final int index = (int)(dataHeader.index & 0xFFFF);

                if (typeId < 0 || typeId >= MAX_TYPES) {
                    // type id is too large or small
                    continue;
                }

                final TentativeTypeHeader typeHeader = newTypeHeaders.computeIfAbsent(typeId, (final int key) -> {
                    return new TentativeTypeHeader();
                });

                final int prevLocation = typeHeader.typeHeader.locations[index];
                if (prevLocation != ABSENT_LOCATION) {
                    // try to skip data if the data is older
                    final long prevTimestamp = typeHeader.timestamps[index];

                    if ((dataHeader.timeWritten - prevTimestamp) <= 0L) {
                        // this data is older, skip it
                        // since we did not validate the data, we cannot skip over the sectors it says it has allocated
                        continue;
                    }
                }

                // read remaining data
                buffer.limit(dataHeader.compressedSize);
                buffer.position(0);
                this.channel.read(buffer, (i << SECTOR_SHIFT) + (long)DataHeader.DATA_HEADER_LENGTH);

                if (buffer.hasRemaining()) {
                    // data is truncated, skip
                    continue;
                }

                buffer.flip();

                // validate data against hash
                final long gotHash = XXHASH64.hash(buffer, 0, dataHeader.compressedSize, XXHASH_SEED);

                if (gotHash != dataHeader.xxhash64Data) {
                    // not the data we expect
                    continue;
                }

                // since we are a newer timestamp than prev, replace it

                final int sectorOffset = (int)i; // i <= MAX_NORMAL_SECTOR_OFFSET
                final int sectorLength = (dataHeader.compressedSize + DataHeader.DATA_HEADER_LENGTH + (SECTOR_SIZE - 1)) >> SECTOR_SHIFT;
                final int newLocation = makeLocation(sectorOffset, sectorLength);

                if (!newSectorAllocation.tryAllocateDirect(sectorOffset, sectorLength, false)) {
                    throw new IllegalStateException("Unable to allocate sectors");
                }

                if (prevLocation != ABSENT_LOCATION && prevLocation != EXTERNAL_ALLOCATION_LOCATION) {
                    newSectorAllocation.freeAllocation(getLocationOffset(prevLocation), getLocationLength(prevLocation));
                }

                typeHeader.typeHeader.locations[index] = newLocation;
                typeHeader.timestamps[index] = dataHeader.timeWritten;

                // skip over the sectors, we know they're good
                i += (long)sectorLength;
                --i;
                continue;
            }
        }

        final IntOpenHashSet possibleTypes = new IntOpenHashSet(128);
        possibleTypes.addAll(this.typeTranslationTable.keySet());
        possibleTypes.addAll(this.typeHeaders.keySet());
        possibleTypes.addAll(newTypeHeaders.keySet());

        // search for external files
        for (final IntIterator iterator = possibleTypes.iterator(); iterator.hasNext();) {
            final int type = iterator.nextInt();
            for (int localZ = 0; localZ < SECTION_SIZE; ++localZ) {
                for (int localX = 0; localX < SECTION_SIZE; ++localX) {
                    final File external = this.getExternalFile(localX, localZ, type);
                    if (!external.isFile()) {
                        continue;
                    }

                    final int index = getIndex(localX, localZ);

                    // read header
                    final DataHeader header;
                    try (final BufferChoices scopedChoices = unscopedBufferChoices.scope();
                        final FileChannel input = FileChannel.open(external.toPath(), StandardOpenOption.READ)) {
                        final ByteBuffer buffer = scopedChoices.t16k().acquireDirectBuffer();

                        buffer.limit(DataHeader.DATA_HEADER_LENGTH);
                        buffer.position(0);

                        input.read(buffer);

                        buffer.flip();

                        header = DataHeader.read(buffer);

                        if (header == null) {
                            // truncated
                            LOGGER.warn("Deleting truncated external file '" + external.getAbsolutePath() + "'");
                            external.delete();
                            continue;
                        }

                        if (!DataHeader.validate(XXHASH64, buffer, 0)) {
                            LOGGER.warn("Failed to verify header hash for external file '" + external.getAbsolutePath() + "'");
                            continue;
                        }
                    } catch (final IOException ex) {
                        LOGGER.warn("Failed to read header from external file '" + external.getAbsolutePath() + "'", ex);
                        continue;
                    }

                    // verify the rest of the header

                    if (type != ((int)header.typeId & 0xFF)) {
                        LOGGER.warn("Mismatch of type and expected type for external file '" + external.getAbsolutePath() + "'");
                        continue;
                    }

                    if (index != ((int)header.index & 0xFFFF)) {
                        LOGGER.warn("Mismatch of index and expected index for external file '" + external.getAbsolutePath() + "'");
                        continue;
                    }

                    if (external.length() != ((long)DataHeader.DATA_HEADER_LENGTH + (long)header.compressedSize)) {
                        LOGGER.warn("Mismatch of filesize and compressed size for external file '" + external.getAbsolutePath() + "'");
                        continue;
                    }

                    // we are mostly certain the data is valid, but need still to check the data hash
                    // we can test the timestamp against current data before the expensive data hash operation though

                    final TentativeTypeHeader typeHeader = newTypeHeaders.computeIfAbsent(type, (final int key) -> {
                        return new TentativeTypeHeader();
                    });

                    final int prevLocation = typeHeader.typeHeader.locations[index];
                    final long prevTimestamp = typeHeader.timestamps[index];

                    if (prevLocation != ABSENT_LOCATION) {
                        if ((header.timeWritten - prevTimestamp) <= 0L) {
                            // this data is older, skip
                            continue;
                        }
                    }

                    // now we can test the hash, after verifying everything else is correct

                    try {
                        final Long externalHash = computeExternalHash(unscopedBufferChoices, external);
                        if (externalHash == null || externalHash.longValue() != header.xxhash64Data) {
                            LOGGER.warn("Failed to verify hash for external file '" + external.getAbsolutePath() + "'");
                            continue;
                        }
                    } catch (final IOException ex) {
                        LOGGER.warn("Failed to compute hash for external file '" + external.getAbsolutePath() + "'", ex);
                        continue;
                    }

                    if (prevLocation != ABSENT_LOCATION && prevLocation != EXTERNAL_ALLOCATION_LOCATION) {
                        newSectorAllocation.freeAllocation(getLocationOffset(prevLocation), getLocationLength(prevLocation));
                    }

                    typeHeader.typeHeader.locations[index] = EXTERNAL_ALLOCATION_LOCATION;
                    typeHeader.timestamps[index] = header.timeWritten;
                }
            }
        }

        // now we can build the new headers
        final Int2ObjectLinkedOpenHashMap<TypeHeader> newHeaders = new Int2ObjectLinkedOpenHashMap<>(newTypeHeaders.size());
        final FileHeader newFileHeader = new FileHeader();

        for (final Iterator<Int2ObjectMap.Entry<TentativeTypeHeader>> iterator = newTypeHeaders.int2ObjectEntrySet().fastIterator(); iterator.hasNext();) {
            final Int2ObjectMap.Entry<TentativeTypeHeader> entry = iterator.next();

            final int type = entry.getIntKey();
            final TentativeTypeHeader tentativeTypeHeader = entry.getValue();

            final int sectorOffset = newSectorAllocation.allocate(TYPE_HEADER_SECTORS, false);
            if (sectorOffset < 0) {
                throw new IllegalStateException("Failed to allocate type header");
            }

            newHeaders.put(type, tentativeTypeHeader.typeHeader);
            newFileHeader.typeHeaderOffsets[type] = sectorOffset;
            // hash will be computed later by writeTypeHeader
        }

        // now print the changes we're about to make
        if ((flags & RECALCULATE_FLAGS_NO_LOG) == 0) {
            LOGGER.info("Summarizing header changes for sectorfile " + this.file.getAbsolutePath());
        }

        boolean changes = false;

        for (final Iterator<Int2ObjectMap.Entry<TypeHeader>> iterator = newHeaders.int2ObjectEntrySet().fastIterator(); iterator.hasNext();) {
            final Int2ObjectMap.Entry<TypeHeader> entry = iterator.next();
            final int type = entry.getIntKey();
            final TypeHeader newTypeHeader = entry.getValue();
            final TypeHeader oldTypeHeader = this.typeHeaders.get(type);

            boolean hasChanges;
            if (oldTypeHeader == null) {
                hasChanges = false;
                final int[] test = newTypeHeader.locations;
                for (int i = 0; i < test.length; ++i) {
                    if (test[i] != ABSENT_LOCATION) {
                        hasChanges = true;
                        break;
                    }
                }
            } else {
                hasChanges = !Arrays.equals(oldTypeHeader.locations, newTypeHeader.locations);
            }

            if (!hasChanges) {
                // make logs easier to read by only logging one line if there are no changes
                if ((flags & RECALCULATE_FLAGS_NO_LOG) == 0) {
                    LOGGER.info("No changes for type " + this.debugType(type) + " in sectorfile " + this.file.getAbsolutePath());
                }
                continue;
            }

            if ((flags & RECALCULATE_FLAGS_NO_LOG) == 0) {
                LOGGER.info("Changes for type " + this.debugType(type) + " in sectorfile '" + this.file.getAbsolutePath() + "':");
            }

            for (int localZ = 0; localZ < SECTION_SIZE; ++localZ) {
                for (int localX = 0; localX < SECTION_SIZE; ++localX) {
                    final int index = getIndex(localX, localZ);

                    final int oldLocation = oldTypeHeader == null ? ABSENT_LOCATION : oldTypeHeader.locations[index];
                    final int newLocation = newTypeHeader.locations[index];

                    if (oldLocation == newLocation) {
                        continue;
                    }

                    changes = true;

                    if ((flags & RECALCULATE_FLAGS_NO_LOG) == 0) {
                        if (oldLocation == ABSENT_LOCATION) {
                            // found new data
                            LOGGER.info("Found missing data for " + this.debugType(type) + " located at " + this.getAbsoluteCoordinate(index) + " in sectorfile " + this.file.getAbsolutePath());
                        } else if (newLocation == ABSENT_LOCATION) {
                            // lost data
                            LOGGER.warn("Failed to find data for " + this.debugType(type) + " located at " + this.getAbsoluteCoordinate(index) + " in sectorfile " + this.file.getAbsolutePath());
                        } else {
                            // changed to last correct data
                            LOGGER.info("Replaced with last good data for " + this.debugType(type) + " located at " + this.getAbsoluteCoordinate(index) + " in sectorfile " + this.file.getAbsolutePath());
                        }
                    }
                }
            }

            if ((flags & RECALCULATE_FLAGS_NO_LOG) == 0) {
                LOGGER.info("End of changes for type " + this.debugType(type) + " in sectorfile " + this.file.getAbsolutePath());
            }
        }
        if ((flags & RECALCULATE_FLAGS_NO_LOG) == 0) {
            LOGGER.info("End of changes for sectorfile " + this.file.getAbsolutePath());
        }

        // replace-in memory
        this.typeHeaders.clear();
        this.typeHeaders.putAll(newHeaders);
        this.fileHeader.copyFrom(newFileHeader);
        this.sectorAllocator.copyAllocations(newSectorAllocation);

        // write to disk
        try {
            // first, the type headers
            for (final IntIterator iterator = newHeaders.keySet().iterator(); iterator.hasNext();) {
                final int type = iterator.nextInt();
                try (final BufferChoices headerBuffers = unscopedBufferChoices.scope()) {
                    try {
                        this.writeTypeHeader(headerBuffers.t16k().acquireDirectBuffer(), type, true, false);
                    } catch (final IOException ex) {
                        // to ensure we update all the type header hashes, we need call writeTypeHeader for all type headers
                        // so, we need to catch any IO errors here
                        LOGGER.error("Failed to write type header " + this.debugType(type) + " to disk for sectorfile " + this.file.getAbsolutePath(), ex);
                    }
                }
            }

            // then we can write the main header
            try (final BufferChoices headerBuffers = unscopedBufferChoices.scope()) {
                this.writeFileHeader(headerBuffers.t16k().acquireDirectBuffer());
            }

            if ((flags & RECALCULATE_FLAGS_NO_LOG) == 0) {
                LOGGER.info("Successfully wrote new headers to disk for sectorfile " + this.file.getAbsolutePath());
            }
        } catch (final IOException ex) {
            LOGGER.error("Failed to write new headers to disk for sectorfile " + this.file.getAbsolutePath(), ex);
        }

        return changes;
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

    private void writeFileHeader(final ByteBuffer ioBuffer) throws IOException {
        ioBuffer.limit(FileHeader.FILE_HEADER_SIZE_BYTES);
        ioBuffer.position(0);

        this.fileHeader.write(ioBuffer.duplicate());

        this.write(ioBuffer, (long)FILE_HEADER_SECTOR << SECTOR_SHIFT);
    }

    private void readFileHeader(final BufferChoices unscopedBufferChoices) throws IOException {
        try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
            final ByteBuffer buffer = scopedBufferChoices.t16k().acquireDirectBuffer();

            // reset sector allocations + headers for debug/testing
            this.sectorAllocator.copyAllocations(newSectorAllocator());
            this.typeHeaders.clear();
            this.fileHeader.reset();

            buffer.limit(FileHeader.FILE_HEADER_SIZE_BYTES);
            buffer.position(0);

            final long fileLengthSectors = (this.channel.size() + (SECTOR_SIZE - 1L)) >> SECTOR_SHIFT;

            int read = this.channel.read(buffer, (long)FILE_HEADER_SECTOR << SECTOR_SHIFT);

            if (read != buffer.limit()) {
                LOGGER.warn("File '" + this.file.getAbsolutePath() + "' has a truncated file header");
                // File is truncated
                // All headers will initialise to default
                return;
            }

            buffer.position(0);

            if (!FileHeader.validate(buffer, 0)) {
                LOGGER.warn("File '" + this.file.getAbsolutePath() + "' has file header with hash mismatch");
                if (!this.readOnly) {
                    this.recalculateFile(unscopedBufferChoices, 0);
                    return;
                } // else: in read-only mode, try to parse the header still
            }

            FileHeader.read(buffer, this.fileHeader);

            // delay recalculation so that the logs contain all errors found
            boolean needsRecalculation = false;

            // try to allocate space for written type headers
            for (int i = 0; i < MAX_TYPES; ++i) {
                final int typeHeaderOffset = this.fileHeader.typeHeaderOffsets[i];
                if (typeHeaderOffset == ABSENT_TYPE_HEADER_OFFSET) {
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

                if (!this.typeTranslationTable.containsKey(i)) {
                    LOGGER.warn("File '" + this.file.getAbsolutePath() + "' has an unknown type header: " + i);
                }

                // parse header
                buffer.position(0);
                buffer.limit(TypeHeader.TYPE_HEADER_SIZE_BYTES);
                read = this.channel.read(buffer, (long)typeHeaderOffset << SECTOR_SHIFT);

                if (read != buffer.limit()) {
                    LOGGER.error("File '" + this.file.getAbsolutePath() + "' has type header " + this.debugType(i) + " pointing to outside of file: " + typeHeaderOffset);
                    needsRecalculation = true;
                    continue;
                }

                final long expectedHash = this.fileHeader.xxHash64TypeHeader[i];
                final long gotHash = TypeHeader.computeHash(buffer, 0);

                if (expectedHash != gotHash) {
                    LOGGER.error("File '" + this.file.getAbsolutePath() + "' has type header " + this.debugType(i) + " with a mismatched hash");
                    needsRecalculation = true;
                    if (!this.readOnly) {
                        continue;
                    } // else: in read-only mode, try to parse the type header still
                }

                final TypeHeader typeHeader = TypeHeader.read(buffer.flip());

                final int[] locations = typeHeader.locations;

                // here, we now will try to allocate space for the data in the type header
                // we need to do it even if we don't know what type we're dealing with
                for (int k = 0; k < locations.length; ++k) {
                    final int location = locations[k];
                    if (location == ABSENT_LOCATION || location == EXTERNAL_ALLOCATION_LOCATION) {
                        // no data or it is on the external file
                        continue;
                    }

                    final int locationOffset = getLocationOffset(location);
                    final int locationLength = getLocationLength(location);

                    if (locationOffset < 0) {
                        LOGGER.error("File '" + this.file.getAbsolutePath() + "' has negative (o:" + locationOffset + ",l:" + locationLength + ") sector offset for type " + this.debugType(i) + " located at " + this.getAbsoluteCoordinate(k));
                        needsRecalculation = true;
                        continue;
                    } else if (locationLength <= 0) {
                        LOGGER.error("File '" + this.file.getAbsolutePath() + "' has negative (o:" + locationOffset + ",l:" + locationLength + ") length for type " + this.debugType(i) + " located at " + this.getAbsoluteCoordinate(k));
                        needsRecalculation = true;
                        continue;
                    } else if ((locationOffset + locationLength) > fileLengthSectors || (locationOffset + locationLength) < 0) {
                        LOGGER.error("File '" + this.file.getAbsolutePath() + "' has sector allocation (o:" + locationOffset + ",l:" + locationLength + ") pointing outside file for type " + this.debugType(i) + " located at " + this.getAbsoluteCoordinate(k));
                        needsRecalculation = true;
                        continue;
                    } else if (!this.sectorAllocator.tryAllocateDirect(locationOffset, locationLength, false)) {
                        LOGGER.error("File '" + this.file.getAbsolutePath() + "' has overlapping sector allocation (o:" + locationOffset + ",l:" + locationLength + ") for type " + this.debugType(i) + " located at " + this.getAbsoluteCoordinate(k));
                        needsRecalculation = true;
                        continue;
                    }
                }

                this.typeHeaders.put(i, typeHeader);
            }

            if (needsRecalculation) {
                this.recalculateFile(unscopedBufferChoices, 0);
                return;
            }

            return;
        }
    }

    private void writeTypeHeader(final ByteBuffer buffer, final int type, final boolean updateTypeHeaderHash,
                                 final boolean writeFileHeader) throws IOException {
        final TypeHeader headerData = this.typeHeaders.get(type);
        if (headerData == null) {
            throw new IllegalStateException("Unhandled type: " + type);
        }

        if (writeFileHeader & !updateTypeHeaderHash) {
            throw new IllegalArgumentException("Cannot write file header without updating type header hash");
        }

        final int offset = this.fileHeader.typeHeaderOffsets[type];

        buffer.position(0);
        buffer.limit(TypeHeader.TYPE_HEADER_SIZE_BYTES);

        headerData.write(buffer.duplicate());

        final long hash;
        if (updateTypeHeaderHash) {
            hash = TypeHeader.computeHash(buffer, 0);
            this.fileHeader.xxHash64TypeHeader[type] = hash;
        }

        this.write(buffer, (long)offset << SECTOR_SHIFT);

        if (writeFileHeader) {
            this.writeFileHeader(buffer);
        }
    }

    private void updateAndWriteTypeHeader(final ByteBuffer ioBuffer, final int type, final int index, final int to) throws IOException {
        final TypeHeader headerData = this.typeHeaders.get(type);
        if (headerData == null) {
            throw new IllegalStateException("Unhandled type: " + type);
        }

        headerData.locations[index] = to;

        this.writeTypeHeader(ioBuffer, type, true, true);
    }

    private void deleteExternalFile(final int localX, final int localZ, final int type) throws IOException {
        // use deleteIfExists for error reporting
        Files.deleteIfExists(this.getExternalFile(localX, localZ, type).toPath());
    }

    private File getExternalFile(final int localX, final int localZ, final int type) {
        return new File(this.file.getParentFile(), getExternalFileName(this.sectionX, this.sectionZ, localX, localZ, type));
    }

    private File getExternalTempFile(final int localX, final int localZ, final int type) {
        return new File(this.file.getParentFile(), getExternalTempFileName(this.sectionX, this.sectionZ, localX, localZ, type));
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
        final int location = typeHeader.locations[index];

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
        this.recalculateFile(scopedBufferChoices, 0);
        // recalculate ensures valid data, so there will be no recursion
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

        final int location = typeHeader.locations[index];

        if (location == ABSENT_LOCATION) {
            return null;
        }

        final boolean external = location == EXTERNAL_ALLOCATION_LOCATION;

        final ByteBufferInputStream rawIn;
        final File externalFile;
        if (external) {
            externalFile = this.getExternalFile(localX, localZ, type);

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
            rawIn.close();
            return this.tryRecalculate("truncated " + (external ? "external" : "internal") + " data header", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
        }

        if ((readFlags & READ_FLAG_CHECK_HEADER_HASH) != 0) {
            if (!DataHeader.validate(XXHASH64, buffer, 0)) {
                rawIn.close();
                return this.tryRecalculate("mismatch of " + (external ? "external" : "internal") + " data header hash", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
            }
        }

        if ((int)(dataHeader.typeId & 0xFF) != type) {
            rawIn.close();
            return this.tryRecalculate("mismatch of expected type and data header type", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
        }

        if (((int)dataHeader.index & 0xFFFF) != index) {
            rawIn.close();
            return this.tryRecalculate("mismatch of expected coordinates and data header coordinates", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
        }

        // this is accurate for our implementations of BufferedFileChannelInputStream / ByteBufferInputStream
        final int bytesAvailable = rawIn.available();

        if (external) {
            // for external files, the remaining size should exactly match the compressed size
            if (bytesAvailable != dataHeader.compressedSize) {
                rawIn.close();
                return this.tryRecalculate("mismatch of external size and data header size", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
            }
        } else {
            // for non-external files, the remaining size should be >= compressed size AND the
            // compressed size should be on the same sector
            if (bytesAvailable < dataHeader.compressedSize || ((bytesAvailable + DataHeader.DATA_HEADER_LENGTH + (SECTOR_SIZE - 1)) >>> SECTOR_SHIFT) != ((dataHeader.compressedSize + DataHeader.DATA_HEADER_LENGTH + (SECTOR_SIZE - 1)) >>> SECTOR_SHIFT)) {
                rawIn.close();
                return this.tryRecalculate("mismatch of internal size and data header size", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
            }
            // adjust max buffer to prevent reading over
            buffer.limit(buffer.position() + dataHeader.compressedSize);
            if (rawIn.available() != dataHeader.compressedSize) {
                // should not be possible
                rawIn.close();
                throw new IllegalStateException();
            }
        }

        final byte compressType = dataHeader.compressionType;
        final SectorFileCompressionType compressionType = SectorFileCompressionType.getById((int)compressType & 0xFF);
        if (compressionType == null) {
            LOGGER.error("File '" + this.file.getAbsolutePath() + "' has unrecognized compression type for data type " + this.debugType(type) + " located at " + this.getAbsoluteCoordinate(index));
            // recalculate will not clobber data types if the compression is unrecognized, so we can only return null here
            rawIn.close();
            return null;
        }

        if (!external && (readFlags & READ_FLAG_CHECK_INTERNAL_DATA_HASH) != 0) {
            final long expectedHash = XXHASH64.hash(buffer, buffer.position(), dataHeader.compressedSize, XXHASH_SEED);
            if (expectedHash != dataHeader.xxhash64Data) {
                rawIn.close();
                return this.tryRecalculate("mismatch of internal data hash and data header hash", scopedBufferChoices, buffer, localX, localZ, type, readFlags);
            }
        } else if (external && (readFlags & READ_FLAG_CHECK_EXTERNAL_DATA_HASH) != 0) {
            final Long externalHash = computeExternalHash(scopedBufferChoices, externalFile);
            if (externalHash == null || externalHash.longValue() != dataHeader.xxhash64Data) {
                rawIn.close();
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
            throw new UnsupportedOperationException("Sectorfile is read-only");
        }

        final TypeHeader typeHeader = this.typeHeaders.get(type);

        if (typeHeader == null) {
            this.checkReadOnlyHeader(type);
            return false;
        }

        final int index = getIndex(localX, localZ);
        final int location = typeHeader.locations[index];

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
            this.deleteExternalFile(localX, localZ, type);

            // no sector allocation to free

            return true;
        } else {
            final int offset = getLocationOffset(location);
            final int length = getLocationLength(location);

            this.sectorAllocator.freeAllocation(offset, length);

            return true;
        }
    }

    // performs a sync as if the sync flag is used for creating the sectorfile
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
            throw new UnsupportedOperationException("Sectorfile is read-only");
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
            // no space left in this file, so we need to make an external allocation

            final File externalTmp = this.getExternalTempFile(localX, localZ, type);
            LOGGER.error("Ran out of space in sectorfile '" + this.file.getAbsolutePath() + "', storing data externally to " + externalTmp.getAbsolutePath());
            Files.deleteIfExists(externalTmp.toPath());

            final FileChannel channel = FileChannel.open(externalTmp.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            try {
                // just need to dump the buffer to the file
                final ByteBuffer bufferDuplicate = buffer.duplicate();
                while (bufferDuplicate.hasRemaining()) {
                    channel.write(bufferDuplicate);
                }

                // this call will write the header again, but that's fine - it's the same data
                this.finishExternalWrite(
                        unscopedBufferChoices, channel, externalTmp, compressedSize, localX, localZ,
                        type, dataHash, compressionType, writeFlags
                );
            } finally {
                channel.close();
                Files.deleteIfExists(externalTmp.toPath());
            }

            return;
        }

        // write data to allocated space
        this.write(buffer, (long)sectorToStore << SECTOR_SHIFT);

        final int prevLocation = this.typeHeaders.get(type).locations[index];

        // update header on disk
        final int newLocation = makeLocation(sectorToStore, requiredSectors);

        try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
            this.updateAndWriteTypeHeader(scopedBufferChoices.t16k().acquireDirectBuffer(), type, index, newLocation);
        }

        // force disk updates if required
        if (!this.sync && (writeFlags & WRITE_FLAG_SYNC) != 0) {
            this.channel.force(false);
        }

        // finally, now we are certain there are no references to the prev location, we can de-allocate
        if (prevLocation != ABSENT_LOCATION) {
            if (prevLocation == EXTERNAL_ALLOCATION_LOCATION) {
                // de-allocation is done by deleting external file
                this.deleteExternalFile(localX, localZ, type);
            } else {
                // just need to free the sector allocation
                this.sectorAllocator.freeAllocation(getLocationOffset(prevLocation), getLocationLength(prevLocation));
            }
        } // else: nothing to free
    }

    private void finishExternalWrite(final BufferChoices unscopedBufferChoices, final FileChannel channel, final File externalTmp,
                                     final int compressedSize, final int localX, final int localZ, final int type, final long dataHash,
                                     final SectorFileCompressionType compressionType, final int writeFlags) throws IOException {
        final int index = getIndex(localX, localZ);

        // update header for external file
        try (final BufferChoices headerChoices = unscopedBufferChoices.scope()) {
            final ByteBuffer buffer = headerChoices.t16k().acquireDirectBuffer();

            buffer.limit(DataHeader.DATA_HEADER_LENGTH);
            buffer.position(0);

            DataHeader.storeHeader(
                    buffer.duplicate(), XXHASH64, dataHash, System.currentTimeMillis(), compressedSize,
                    (short)index, (byte)type, (byte)compressionType.getId()
            );

            int offset = 0;
            while (buffer.hasRemaining()) {
                offset += channel.write(buffer, (long)offset);
            }
        }

        // replace existing external file

        final File external = this.getExternalFile(localX, localZ, type);

        if (this.sync || (writeFlags & WRITE_FLAG_SYNC) != 0) {
            channel.force(true);
        }
        channel.close();
        try {
            Files.move(externalTmp.toPath(), external.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (final AtomicMoveNotSupportedException ex) {
            Files.move(externalTmp.toPath(), external.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }

        final int prevLocation = this.typeHeaders.get(type).locations[index];

        // update header on disk if required

        if (prevLocation != EXTERNAL_ALLOCATION_LOCATION) {
            try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
                this.updateAndWriteTypeHeader(scopedBufferChoices.t16k().acquireDirectBuffer(), type, index, EXTERNAL_ALLOCATION_LOCATION);
            }

            // force disk updates if required
            if (!this.sync && (writeFlags & WRITE_FLAG_SYNC) != 0) {
                this.channel.force(false);
            }
        }

        // finally, now we are certain there are no references to the prev location, we can de-allocate
        if (prevLocation != ABSENT_LOCATION && prevLocation != EXTERNAL_ALLOCATION_LOCATION) {
            this.sectorAllocator.freeAllocation(getLocationOffset(prevLocation), getLocationLength(prevLocation));
        }

        LOGGER.warn("Stored externally " + external.length() + " bytes for type " + this.debugType(type) + " to file " + external.getAbsolutePath());
    }

    public final class CoordinateIndexedSectorFileOutput extends ByteBufferOutputStream {
        private final BufferChoices scopedBufferChoices;

        private File externalFile;
        private FileChannel externalChannel;
        private StreamingXXHash64 externalHash;
        private int totalCompressedSize;

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
            for (int i = 0; i < DataHeader.DATA_HEADER_LENGTH; ++i) {
                this.buffer.put(i, (byte)0);
            }
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
            if (this.externalFile == null && current.hasRemaining()) {
                return current;
            }
            if (current.position() == 0) {
                // nothing to do
                return current;
            }

            final boolean firstWrite = this.externalFile == null;

            if (firstWrite) {
                final File externalTmpFile = SectorFile.this.getExternalTempFile(this.localX, this.localZ, this.type);
                LOGGER.warn("Storing external data at " + externalTmpFile.getAbsolutePath());
                Files.deleteIfExists(externalTmpFile.toPath());

                this.externalFile = externalTmpFile;
                this.externalChannel = FileChannel.open(externalTmpFile.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                this.externalHash = XXHASH_JAVA_FACTORY.newStreamingHash64(XXHASH_SEED);
            }

            this.totalCompressedSize += (firstWrite ? current.position() - DataHeader.DATA_HEADER_LENGTH : current.position());

            if (this.totalCompressedSize < 0 || this.totalCompressedSize >= (Integer.MAX_VALUE - DataHeader.DATA_HEADER_LENGTH)) {
                // too large
                throw new IOException("External file length exceeds integer maximum");
            }

            current.flip();

            // update data hash
            try (final BufferChoices hashChoices = this.scopedBufferChoices.scope()) {
                final byte[] bytes = hashChoices.t16k().acquireJavaBuffer();

                int offset = firstWrite ? DataHeader.DATA_HEADER_LENGTH : 0;
                final int len = current.limit();

                while (offset < len) {
                    final int maxCopy = Math.min(len - offset, bytes.length);

                    current.get(offset, bytes, 0, maxCopy);
                    offset += maxCopy;

                    this.externalHash.update(bytes, 0, maxCopy);
                }
            }

            // update on disk
            while (current.hasRemaining()) {
                this.externalChannel.write(current);
            }

            current.limit(current.capacity());
            current.position(0);
            return current;
        }

        // assume flush() is called before this
        private void save() throws IOException {
            if (this.externalFile == null) {
                // avoid clobbering buffer positions/limits
                final ByteBuffer buffer = this.buffer.duplicate();

                buffer.flip();

                final long dataHash = XXHASH64.hash(
                        this.buffer, DataHeader.DATA_HEADER_LENGTH, buffer.remaining() - DataHeader.DATA_HEADER_LENGTH,
                        XXHASH_SEED
                );

                SectorFile.this.writeInternal(
                        this.scopedBufferChoices, buffer, this.localX, this.localZ,
                        this.type, dataHash, this.compressionType, this.writeFlags
                );
            } else {
                SectorFile.this.finishExternalWrite(
                        this.scopedBufferChoices, this.externalChannel, this.externalFile, this.totalCompressedSize,
                        this.localX, this.localZ, this.type, this.externalHash.getValue(), this.compressionType,
                        this.writeFlags
                );
            }
        }

        public void freeResources() throws IOException {
            if (this.externalHash != null) {
                this.externalHash.close();
                this.externalHash = null;
            }
            if (this.externalChannel != null) {
                this.externalChannel.close();
                this.externalChannel = null;
            }
            if (this.externalFile != null) {
                // only deletes tmp file if we did not call save()
                this.externalFile.delete();
                this.externalFile = null;
            }
        }

        @Override
        public void close() throws IOException {
            try {
                this.flush();
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
            this.flush();
        } finally {
            this.channel.close();
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
            final int fromEnd = fromStart + getFreeBlockLength(fromBlock) - 1;

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
            // add merged free block
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
