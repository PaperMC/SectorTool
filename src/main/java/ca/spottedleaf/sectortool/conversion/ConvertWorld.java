package ca.spottedleaf.sectortool.conversion;

import ca.spottedleaf.io.region.SectorFile;
import ca.spottedleaf.io.region.MinecraftRegionFileType;
import ca.spottedleaf.io.region.SectorFileCompressionType;
import ca.spottedleaf.io.buffer.BufferChoices;
import ca.spottedleaf.sectortool.Main;
import ca.spottedleaf.sectortool.storage.RegionFile;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public final class ConvertWorld {

    private static final String[] SUBDIRS = new String[] {
            "",
            "DIM-1",
            "DIM1"
    };

    private static final String TARGET_DIRECTORY = "sectors";

    public static final String INPUT_PROPERTY = "input";
    public static final String TYPE_OUTPUT_PROPERTY = "compress";

    public static final int COMPRESSION_TYPE_COPY_ID = -1;

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
        final boolean raw = compressionType == null;
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
                SectorFile.getFileName(sectionX, sectionZ)
        );

        final SectorFile outputFile;
        try {
            outputFile = new SectorFile(
                    output, sectionX, sectionZ, raw ? SectorFileCompressionType.NONE : compressionType, unscopedBufferChoices,
                    MinecraftRegionFileType.getTranslationTable(),
                    0
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
                byId.put(type.getNewId(), new RegionFile(file, sectionX, sectionZ, unscopedBufferChoices, true));
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
                try (final BufferChoices readScope = unscopedBufferChoices.scope()) {
                    for (final RegionFile regionFile : byId.values()) {
                        try {
                            regionFile.fillRaw(readScope);
                        } catch (final IOException ex) {
                            synchronized (System.err) {
                                System.err.println("Failed to read raw from regionfile " + regionFile.file.getAbsolutePath());
                                ex.printStackTrace(System.err);
                            }
                        }
                    }
                }

                try (final BufferChoices readScope = unscopedBufferChoices.scope()) {
                    final RegionFile.CustomByteArrayOutputStream decompressed = new RegionFile.CustomByteArrayOutputStream(readScope.t1m().acquireJavaBuffer());

                    for (int i = 0; i < 32 * 32; ++i) {
                        final int chunkX = (i & 31);
                        final int chunkZ = ((i >>> 5) & 31);

                        for (final MinecraftRegionFileType type : MinecraftRegionFileType.getAll()) {
                            final RegionFile regionFile = byId.get(type.getNewId());
                            if (regionFile == null) {
                                continue;
                            }

                            decompressed.reset();

                            int read = -1;
                            try {
                                read = regionFile.read(chunkX, chunkZ, readScope, decompressed, raw);
                            } catch (final IOException ex) {
                                synchronized (System.err) {
                                    System.err.println(
                                            "Failed to read " + type.getName() + " (" + chunkX + "," + chunkZ + ") from regionfile " +
                                                    regionFile.file.getAbsolutePath() + ": ");
                                    ex.printStackTrace(System.err);
                                }
                            }

                            if (read < 0) {
                                continue;
                            }

                            try (final BufferChoices writeScope = readScope.scope();) {
                               final SectorFile.SectorFileOutput output = outputFile.write(
                                       writeScope, chunkX, chunkZ, type.getNewId(),
                                       raw ? SectorFileCompressionType.fromRegionFile(read) : null,
                                       (raw ? SectorFile.WRITE_FLAG_RAW : 0)
                               );
                               try {
                                   decompressed.writeTo(output.outputStream());
                                   output.outputStream().close();
                               } finally {
                                   output.rawOutput().freeResources();
                               }
                            } catch (final IOException ex) {
                                synchronized (System.err) {
                                    System.err.println(
                                            "Failed to write " + type.getName() + " (" + chunkX + "," + chunkZ + ") from regionfile " +
                                                    regionFile.file.getAbsolutePath() + " to sectorfile " + output.getAbsolutePath() + ": ");
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

        final int compressionTypeId = Integer.getInteger(TYPE_OUTPUT_PROPERTY, -1);
        final SectorFileCompressionType compressionType = SectorFileCompressionType.getById(compressionTypeId);
        if (compressionType == null && compressionTypeId != COMPRESSION_TYPE_COPY_ID) {
            System.err.println("Specified compression type is absent (-D" + TYPE_OUTPUT_PROPERTY + "=<id>) or invalid");
            System.err.println("Select one from: 1 (GZIP), 2 (DEFLATE), 3 (NONE), 4 (LZ4), 5 (ZSTD)");
            return;
        }

        final BufferChoices bufferChoices = BufferChoices.createNew(Main.THREADS * 20);

        final ExecutorService executors = createExecutors();

        final AtomicInteger concurrentExecutions = new AtomicInteger();
        // avoid opening too many files by limiting the number of concurrently executing tasks
        final int targetConcurrent = Main.THREADS * 4;
        final int waitThreshold = targetConcurrent / 2;

        for (final String subdir : SUBDIRS) {
            System.out.println("Converting dimension " + (subdir.isEmpty() ? "overworld" : subdir));

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

            System.out.println("Converted dimension " + (subdir.isEmpty() ? "overworld" : subdir));
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
