package ca.spottedleaf.regioncompresstest.fuzz;

import ca.spottedleaf.io.buffer.BufferChoices;
import ca.spottedleaf.io.region.SectorFile;
import ca.spottedleaf.io.region.SectorFileCompressionType;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.DataInputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Random;

public final class FuzzProfilingTest {

    private static final BufferChoices BUFFER_CHOICES = BufferChoices.createNew(10);
    private static final Int2ObjectLinkedOpenHashMap<String> FUZZ_TYPES = new Int2ObjectLinkedOpenHashMap<>(SectorFile.MAX_TYPES);
    static {
        for (int i = 0; i < SectorFile.MAX_TYPES; ++i) {
            FUZZ_TYPES.put(i, Integer.toString(i));
        }
    }

    public static final String OUTPUT_DIR_PROPERTY = "output";
    private static final SectorFileCompressionType COMPRESSION_TYPE = SectorFileCompressionType.ZSTD;

    private static File TEST_DIR;

    private static final Logger LOGGER = LoggerFactory.getLogger(SectorFile.class);

    private static long mix(final int x, final int z, final long seed, final int type) {
        return HashCommon.mix((((long)z << 32) | (x & 0xFFFFFFFFL)) ^ seed) + type;
    }

    private static final int FLAGS_ALWAYS_HAS       = 1 << 0;
    private static final int FLAGS_NEVER_HAS        = 1 << 1;
    private static final int FLAGS_ALWAYS_OVERSIZED = 1 << 2;
    private static final int FLAGS_HAS_OVERSIZED    = 1 << 3;
    private static final int FLAGS_FORCE_BAD_SECTOR = 1 << 4;

    private static final int MAX_SECTORS_UNDERSIZED = (1 << 11) - 1;
    private static final int MAX_BYTES_UNDERSIZED = SectorFile.SECTOR_SIZE * MAX_SECTORS_UNDERSIZED;

    private static final double DATA_CHANCE = 0.79;
    private static final double OVERSIZED_CHANCE = 0.10;

    private static byte[] generateData(final int x, final int z, final int type, final long seed, final int flags) {
        if ((flags & FLAGS_NEVER_HAS) != 0) {
            return null;
        }

        final long coordSeed = mix(x, z, seed, type);
        final Random random = new Random(coordSeed);

        if ((flags & FLAGS_ALWAYS_HAS) == 0) {
            if (random.nextDouble() < DATA_CHANCE) {
                return null;
            }
        }

        int dataSize = random.nextInt(MAX_BYTES_UNDERSIZED) + 1;

        if ((flags & FLAGS_ALWAYS_OVERSIZED) != 0) {
            dataSize += MAX_BYTES_UNDERSIZED + MAX_BYTES_UNDERSIZED*random.nextInt(10 + 1);
        } else if ((flags & FLAGS_HAS_OVERSIZED) != 0) {
            if (random.nextDouble() < OVERSIZED_CHANCE) {
                dataSize += MAX_BYTES_UNDERSIZED + MAX_BYTES_UNDERSIZED*random.nextInt(10 + 1);
            }
        }

        final byte[] ret = new byte[dataSize];
        random.nextBytes(ret);
        return ret;
    }

    private static byte[] doRead(final SectorFile sectorFile, final int x, final int z, final int type) {
        try (final BufferChoices scopedBufferChoices = BUFFER_CHOICES.scope();
             final DataInputStream in = sectorFile.read(scopedBufferChoices, x & SectorFile.SECTION_MASK, z & SectorFile.SECTION_MASK, type, SectorFile.FULL_VALIDATION_FLAGS);) {

            return in == null ? null : in.readAllBytes();
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void doWrite(final SectorFile sectorFile, final int x, final int z, final int type,
                                final byte[] data, final int doff, final int dlen) {
        try (final BufferChoices scopedBufferChoices = BUFFER_CHOICES.scope();) {
            if (data == null) {
                sectorFile.delete(scopedBufferChoices, x & SectorFile.SECTION_MASK, z & SectorFile.SECTION_MASK, type);
                return;
            }

            final SectorFile.SectorFileOutput output = sectorFile.write(
                scopedBufferChoices, x & SectorFile.SECTION_MASK, z & SectorFile.SECTION_MASK, type,
                null, 0
            );

            try {
                output.outputStream().write(data, doff, dlen);
                output.outputStream().close();
            } finally {
                output.rawOutput().freeResources();
            }
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void testRead(final SectorFile sectorFile, final int baseX, final int baseZ,
                                 final int type, final long seed, final int flags) {
        for (int localZ = 0; localZ < SectorFile.SECTION_SIZE; ++localZ) {
            for (int localX = 0; localX < SectorFile.SECTION_SIZE; ++localX) {
                final int absoluteX = localX | (baseX << SectorFile.SECTION_SHIFT);
                final int absoluteZ = localZ | (baseZ << SectorFile.SECTION_SHIFT);

                final byte[] expected = generateData(absoluteX, absoluteZ, type, seed, flags);
                final byte[] got = doRead(sectorFile, absoluteX, absoluteZ, type);

                if (Arrays.equals(got, expected)) {
                    continue;
                }

                LOGGER.error("Failed read for (" + absoluteX + "," + absoluteZ + ")");
            }
        }
    }

    private static void setupForRead(final SectorFile sectorFile, final int baseX, final int baseZ,
                                     final int type, final long seed, final int flags) {
        for (int localZ = 0; localZ < SectorFile.SECTION_SIZE; ++localZ) {
            for (int localX = 0; localX < SectorFile.SECTION_SIZE; ++localX) {
                final int absoluteX = localX | (baseX << SectorFile.SECTION_SHIFT);
                final int absoluteZ = localZ | (baseZ << SectorFile.SECTION_SHIFT);

                final byte[] expected = generateData(absoluteX, absoluteZ, type, seed, flags);

                doWrite(sectorFile, absoluteX, absoluteZ, type, expected, 0, (expected == null ? 0 : expected.length));
            }
        }
    }

    private static void doTest(final int sectionX, final int sectionZ, final int type, final long seed, final int flags) {
        try {
            final File file = new File(TEST_DIR, SectorFile.getFileName(sectionX, sectionZ));

            final SectorFile sectorfile = new SectorFile(
                file, sectionX, sectionZ, COMPRESSION_TYPE, BUFFER_CHOICES, FUZZ_TYPES, 0
            );

            if ((flags & FLAGS_FORCE_BAD_SECTOR) != 0) {
                while (sectorfile.forTestingAllocateSector(MAX_SECTORS_UNDERSIZED) > 0);
            }

            setupForRead(sectorfile, sectionX, sectionZ, type, seed, flags);

            testRead(sectorfile, sectionX, sectionZ, type, seed, flags);

            if (sectorfile.recalculateFile(BUFFER_CHOICES, SectorFile.RECALCULATE_FLAGS_NO_LOG | SectorFile.RECALCULATE_FLAGS_NO_BACKUP)) {
                LOGGER.error("Should have no changes");
            }

            testRead(sectorfile, sectionX, sectionZ, type, seed, flags);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void run(final String[] args) {
        final String outputDirPath = System.getProperty(OUTPUT_DIR_PROPERTY);
        if (outputDirPath == null) {
            System.err.println("Must specify output directory as as -D" + OUTPUT_DIR_PROPERTY + "=<path>");
            return;
        }

        TEST_DIR = new File(outputDirPath);
        if (!TEST_DIR.exists()) {
            TEST_DIR.mkdirs();
        } else if (!TEST_DIR.isDirectory()) {
            System.err.println("Output directory '" + outputDirPath + "' is not a directory");
            return;
        }

        doTest(630, 20, 0, 0L, 0);
        doTest(1050, 43245, 1, 0L, FLAGS_ALWAYS_OVERSIZED);
        doTest(1423324, 4234234, 2, 0L, FLAGS_ALWAYS_HAS);
        doTest(14324, 42234, 3, 0L, FLAGS_ALWAYS_HAS | FLAGS_ALWAYS_OVERSIZED);
        doTest(1, 4234234, 4, 0L, FLAGS_ALWAYS_HAS | FLAGS_HAS_OVERSIZED);
        doTest(133, 4234, 4, 0L, FLAGS_HAS_OVERSIZED);


        for (int i = SectorFile.MAX_TYPES-1; i > (SectorFile.MAX_TYPES-1-24); --i) {
            doTest(0, 0, i, 0L, FLAGS_HAS_OVERSIZED);
        }

        doTest(3241, 43256, 3, 0L, FLAGS_FORCE_BAD_SECTOR | FLAGS_ALWAYS_HAS | FLAGS_HAS_OVERSIZED);
    }
}
