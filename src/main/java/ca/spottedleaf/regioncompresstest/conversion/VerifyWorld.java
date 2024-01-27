package ca.spottedleaf.regioncompresstest.conversion;

import ca.spottedleaf.io.region.CoordinateIndexedSectorFile;
import ca.spottedleaf.io.region.MinecraftRegionFileType;
import ca.spottedleaf.io.region.SectorFileCompressionType;
import ca.spottedleaf.io.buffer.BufferChoices;
import ca.spottedleaf.regioncompresstest.Main;
import ca.spottedleaf.regioncompresstest.storage.RegionFile;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public final class VerifyWorld {

    private static final String[] SUBDIRS = new String[] {
            "",
            "DIM-1",
            "DIM1"
    };

    private static final String TARGET_DIRECTORY = "sectors";

    public static final String INPUT_PROPERTY = "input";
    public static final String TYPE_OUTPUT_PROPERTY = "compress";

    private static ExecutorService createExecutors() {
        return Executors.newFixedThreadPool(Main.THREADS, new ThreadFactory() {
            private final AtomicInteger idGenerator = new AtomicInteger();

            @Override
            public Thread newThread(final Runnable run) {
                final Thread ret = new Thread(run);

                ret.setName("Conversion worker #" + this.idGenerator.getAndIncrement());
                ret.setUncaughtExceptionHandler((final Thread thread, final Throwable ex) -> {
                    synchronized (System.err) {
                        System.err.println("Thread " + thread.getName() + " threw uncaught exception: ");
                        ex.printStackTrace(System.err);
                    }
                });

                return ret;
            }
        });
    }

    private static void decrementTracker(final AtomicInteger concurrentTracker, final Thread wakeup, final int threshold) {
        final int count = concurrentTracker.decrementAndGet();
        if (count == threshold || count == 0) {
            LockSupport.unpark(wakeup);
        }
    }

    private static void submitToExecutor(final ExecutorService executor, final File dimDirectory, final String regionName,
                                         final SectorFileCompressionType compressionType, final BufferChoices unscopedBufferChoices,
                                         final AtomicInteger concurrentTracker, final Thread wakeup, final int threshold) {
        // old format is r.<x>.<z>.mca

        final String[] coords = regionName.substring(2, regionName.length() - RegionFile.ANVIL_EXTENSION.length()).split("\\.");

        final int sectionX;
        final int sectionZ;

        try {
            sectionX = Integer.parseInt(coords[0]);
            sectionZ = Integer.parseInt(coords[1]);
        } catch (final NumberFormatException ex) {
            System.err.println("Invalid region name: " + regionName);
            return;
        }

        final File output = new File(
                new File(dimDirectory, TARGET_DIRECTORY),
                CoordinateIndexedSectorFile.getFileName(sectionX, sectionZ)
        );

        final CoordinateIndexedSectorFile outputFile;
        try {
            outputFile = new CoordinateIndexedSectorFile(
                    output, sectionX, sectionZ, false, compressionType, unscopedBufferChoices,
                    MinecraftRegionFileType.getTranslationTable(),
                    false
            );
        } catch (final IOException ex) {
            synchronized (System.err) {
                System.err.println("Failed to create sector file " + output.getAbsolutePath() + ": ");
                ex.printStackTrace(System.err);
            }
            return;
        }

        final Int2ObjectLinkedOpenHashMap<RegionFile> byId = new Int2ObjectLinkedOpenHashMap<>();

        for (final MinecraftRegionFileType type : MinecraftRegionFileType.getAll()) {
            final File file = new File(
                    new File(dimDirectory, type.getFolder()),
                    regionName
            );
            if (!file.isFile()) {
                continue;
            }
            try {
                byId.put(type.getNewId(), new RegionFile(file, unscopedBufferChoices));
            } catch (final IOException ex) {
                synchronized (System.err) {
                    System.err.println("Failed to open regionfile " + file.getAbsolutePath() + ": ");
                    ex.printStackTrace(System.err);
                }
            }
        }

        concurrentTracker.getAndIncrement();

        class RegionFileReader implements Runnable {

            @Override
            public void run() {
                try (final BufferChoices regionReadScope = unscopedBufferChoices.scope()) {
                    final RegionFile.CustomByteArrayOutputStream decompressed = new RegionFile.CustomByteArrayOutputStream(regionReadScope.t1m().acquireJavaBuffer());

                    for (int i = 0; i < 32 * 32; ++i) {
                        final int chunkX = (i & 31);
                        final int chunkZ = ((i >>> 5) & 31);

                        decompressed.reset();

                        for (final MinecraftRegionFileType type : MinecraftRegionFileType.getAll()) {
                            final RegionFile regionFile = byId.get(type.getNewId());
                            if (regionFile == null) {
                                continue;
                            }

                            boolean read = false;
                            try {
                                read = regionFile.read(chunkX, chunkZ, regionReadScope, decompressed);
                            } catch (final IOException ex) {
                                synchronized (System.err) {
                                    System.err.println(
                                            "Failed to read " + type.getName() + " (" + chunkX + "," + chunkZ + ") from regionfile " +
                                                    regionFile.file.getAbsolutePath() + ": ");
                                    ex.printStackTrace(System.err);
                                }
                            }

                            if (!read) {
                                continue;
                            }

                            try (final BufferChoices sectorReadScope = regionReadScope.scope();) {
                                final DataInputStream is = outputFile.read(sectorReadScope, chunkX, chunkZ, type.getNewId(), CoordinateIndexedSectorFile.FULL_VALIDATION_FLAGS);

                                if (is == null) {
                                    // TODO check sectorfile exists but not region
                                    continue; //throw new IOException("Does not exist on sector file");
                                }

                                final byte[] bytes = new byte[decompressed.size()];

                                int len = 0;
                                int r;
                                while (len < bytes.length && (r = is.read(bytes, len, bytes.length - len)) >= 0) {
                                    len += r;
                                }

                                if (len != bytes.length) {
                                    throw new IOException("Too small: got " + (len) + " compared to " + bytes.length);
                                }

                                if (is.read() != -1) {
                                    throw new IOException("Too large: got " + (is.available() + len + 1) + " compared to " + bytes.length);
                                }

                                if (!Arrays.equals(bytes, 0, bytes.length, decompressed.getBuffer(), 0, decompressed.size())) {
                                    throw new IOException("Unequal data");
                                }
                            } catch (final IOException ex) {
                                synchronized (System.err) {
                                    System.err.println(
                                            "Failed to read " + type.getName() + " (" + chunkX + "," + chunkZ +
                                                ") from sectorfile " + output.getAbsolutePath() + ": ");
                                    ex.printStackTrace(System.err);
                                }
                            }
                        }
                    }
                } finally {
                    decrementTracker(concurrentTracker, wakeup, threshold);

                    try {
                        outputFile.close();
                    } catch (final IOException ex) {
                        synchronized (System.err) {
                            System.err.println("Failed to close sectorfile " + outputFile.file.getAbsolutePath() + ": ");
                            ex.printStackTrace(System.err);
                        }
                    }
                    for (final RegionFile regionFile : byId.values()) {
                        try {
                            regionFile.close();
                        } catch (final IOException ex) {
                            synchronized (System.err) {
                                System.err.println("Failed to close regionfile " + regionFile.file.getAbsolutePath() + ": ");
                                ex.printStackTrace(System.err);
                            }
                        }
                    }
                }

                System.out.println("Processed sectorfile " + outputFile.file.getAbsolutePath());
            }
        }

        executor.execute(new RegionFileReader());
    }

    public static void run(final String[] args) {
        final String inputDirectoryPath = System.getProperty(INPUT_PROPERTY);
        if (inputDirectoryPath == null) {
            System.err.println("Must specify input (directory or region file) as -D" + INPUT_PROPERTY + "=<path>");
            return;
        }
        final File inputDir = new File(inputDirectoryPath);
        if (!inputDir.isDirectory()) {
            System.err.println("Specified input is not a directory or .mca file");
            return;
        }

        final SectorFileCompressionType compressionType = SectorFileCompressionType.getById(Integer.getInteger(TYPE_OUTPUT_PROPERTY, -1));
        if (compressionType == null) {
            System.err.println("Specified compression type is absent (-D" + TYPE_OUTPUT_PROPERTY + "=<id>) or invalid");
            System.err.println("Select one from: 1 (GZIP), 2 (DEFLATE), 3 (NONE), 4 (LZ4)");
            return;
        }

        final BufferChoices bufferChoices = BufferChoices.createNew(Main.THREADS * 20);

        final ExecutorService executors = createExecutors();

        final AtomicInteger concurrentExecutions = new AtomicInteger();
        // avoid opening too many files by limiting the number of concurrently executing tasks
        final int targetConcurrent = Main.THREADS * 4;
        final int waitThreshold = targetConcurrent / 2;

        for (final String subdir : SUBDIRS) {
            System.out.println("Verifying dimension " + (subdir.isEmpty() ? "overworld" : subdir));

            final File dimDirectory = subdir.isEmpty() ? inputDir : new File(inputDir, subdir);

            if (!dimDirectory.isDirectory()) {
                System.out.println("Skipping dimension " + (subdir.isEmpty() ? "overworld" : subdir) + ", it is empty");
                continue;
            }

            final Set<String> toConvert = new LinkedHashSet<>();

            for (final MinecraftRegionFileType type : MinecraftRegionFileType.getAll()) {
                final File regionDir = new File(dimDirectory, type.getFolder());

                if (!regionDir.isDirectory()) {
                    System.out.println("Skipping type " + type.getFolder() + ", it is empty");
                    continue;
                }

                for (final String name : regionDir.list((final File dir, final String name) -> {
                    return name.endsWith(RegionFile.ANVIL_EXTENSION);
                })) {
                    toConvert.add(name);
                }
            }

            if (toConvert.isEmpty()) {
                System.out.println("No regions in " + dimDirectory.getAbsolutePath());
                continue;
            }

            new File(dimDirectory, TARGET_DIRECTORY).mkdir();

            for (final String convert : toConvert) {
                submitToExecutor(executors, dimDirectory, convert, compressionType, bufferChoices, concurrentExecutions, Thread.currentThread(), waitThreshold);

                if (concurrentExecutions.get() >= targetConcurrent) {
                    while (concurrentExecutions.get() > waitThreshold) {
                        LockSupport.park("Awaiting for max concurrent");
                    }
                }
            }

            while (concurrentExecutions.get() > 0) {
                LockSupport.park("Awaiting finish");
            }

            System.out.println("Verified dimension " + (subdir.isEmpty() ? "overworld" : subdir));
        }

        while (concurrentExecutions.get() > 0) {
            LockSupport.park("Awaiting finish");
        }

        executors.shutdown();
        try {
            executors.awaitTermination(1, TimeUnit.MINUTES);
        } catch (final InterruptedException ignore) {}
    }
}
