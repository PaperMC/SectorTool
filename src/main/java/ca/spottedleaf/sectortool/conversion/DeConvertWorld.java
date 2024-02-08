package ca.spottedleaf.sectortool.conversion;

import ca.spottedleaf.io.region.SectorFile;
import ca.spottedleaf.io.region.MinecraftRegionFileType;
import ca.spottedleaf.io.region.SectorFileCompressionType;
import ca.spottedleaf.io.buffer.BufferChoices;
import ca.spottedleaf.sectortool.Main;
import ca.spottedleaf.sectortool.storage.RegionFile;
import java.io.DataInputStream;
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

public final class DeConvertWorld {

    private static final String[] SUBDIRS = new String[] {
            "",
            "DIM-1",
            "DIM1"
    };

    private static final String SOURCE_DIRECTORY = "sectors";

    public static final String INPUT_PROPERTY = "input";
    public static final String TYPE_OUTPUT_PROPERTY = "compress";

    private static ExecutorService createExecutors() {
        return Executors.newFixedThreadPool(Main.THREADS, new ThreadFactory() {
            private final AtomicInteger idGenerator = new AtomicInteger();

            @Override
            public Thread newThread(final Runnable run) {
                final Thread ret = new Thread(run);

                ret.setName("DeConversion worker #" + this.idGenerator.getAndIncrement());
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

    private static void submitToExecutor(final ExecutorService executor, final File dimDirectory, final String sectorName,
                                         final int compressionType, final BufferChoices unscopedBufferChoices,
                                         final AtomicInteger concurrentTracker, final Thread wakeup, final int threshold) {
        // new format is <x>.<z>.sf

        final String[] coords = sectorName.substring(0, sectorName.length() - SectorFile.FILE_EXTENSION.length()).split("\\.");

        final int sectionX;
        final int sectionZ;

        try {
            sectionX = Integer.parseInt(coords[0]);
            sectionZ = Integer.parseInt(coords[1]);
        } catch (final NumberFormatException ex) {
            System.err.println("Invalid sector name: " + sectorName);
            return;
        }

        final File input = new File(
                new File(dimDirectory, SOURCE_DIRECTORY),
                SectorFile.getFileName(sectionX, sectionZ)
        );

        final SectorFile inputFile;
        try {
            inputFile = new SectorFile(
                    input, sectionX, sectionZ, SectorFileCompressionType.GZIP, unscopedBufferChoices,
                    MinecraftRegionFileType.getTranslationTable(),
                    SectorFile.OPEN_FLAGS_READ_ONLY
            );
        } catch (final IOException ex) {
            synchronized (System.err) {
                System.err.println("Failed to create sector file " + input.getAbsolutePath() + ": ");
                ex.printStackTrace(System.err);
            }
            return;
        }

        concurrentTracker.getAndIncrement();

        class RegionFileReader implements Runnable {

            @Override
            public void run() {
                try (final BufferChoices readScope = unscopedBufferChoices.scope()) {
                    final RegionFile.CustomByteArrayOutputStream decompressed = new RegionFile.CustomByteArrayOutputStream(readScope.t1m().acquireJavaBuffer());
                    final byte[] buffer = readScope.t16k().acquireJavaBuffer();

                    for (final MinecraftRegionFileType type : MinecraftRegionFileType.getAll()) {
                        final File file = new File(
                                new File(dimDirectory, type.getFolder()),
                                "r." + sectionX + "." + sectionZ + ".mca"
                        );

                        final RegionFile regionFile;
                        try {
                            regionFile = new RegionFile(file, sectionX, sectionZ, unscopedBufferChoices, false);
                        } catch (final IOException ex) {
                            synchronized (System.err) {
                                System.err.println("Failed to close regionfile " + file.getAbsolutePath() + ": ");
                                ex.printStackTrace(System.err);
                            }
                            continue;
                        }

                        for (int i = 0; i < (32*32); ++i) {
                            final int chunkX = (i & 31);
                            final int chunkZ = ((i >>> 5) & 31);

                            try (final BufferChoices readChoices = unscopedBufferChoices.scope();
                                 final DataInputStream is = inputFile.read(readChoices, chunkX, chunkZ, type.getNewId(), SectorFile.FULL_VALIDATION_FLAGS);) {

                                if (is == null) {
                                    regionFile.delete(chunkX, chunkZ, unscopedBufferChoices);
                                    continue;
                                }

                                decompressed.reset();

                                int r;
                                while ((r = is.read(buffer)) >= 0) {
                                    decompressed.write(buffer, 0, r);
                                }
                            } catch (final IOException ex) {
                                synchronized (System.err) {
                                    System.err.println(
                                            "Failed to read " + type.getName() + " (" + chunkX + "," + chunkZ
                                                    + ") from sectorfile " + input.getAbsolutePath() + ": ");
                                    ex.printStackTrace(System.err);
                                }
                            }

                            try {
                                regionFile.write(
                                        chunkX, chunkZ, unscopedBufferChoices, compressionType,
                                        decompressed.getBuffer(), 0, decompressed.size()
                                );
                            } catch (final IOException ex) {
                                synchronized (System.err) {
                                    System.err.println(
                                            "Failed to write " + type.getName() + " (" + chunkX + "," + chunkZ + ") to regionfile " +
                                                    regionFile.file.getAbsolutePath() + " from sectorfile " + file.getAbsolutePath() + ": ");
                                    ex.printStackTrace(System.err);
                                }
                            }
                        }

                        try {
                            regionFile.close();
                        } catch (final IOException ex) {
                            synchronized (System.err) {
                                System.err.println("Failed to close regionfile " + regionFile.file.getAbsolutePath() + ": ");
                                ex.printStackTrace(System.err);
                            }
                        }
                    }
                } finally {
                    decrementTracker(concurrentTracker, wakeup, threshold);

                    try {
                        inputFile.close();
                    } catch (final IOException ex) {
                        synchronized (System.err) {
                            System.err.println("Failed to close sectorfile " + inputFile.file.getAbsolutePath() + ": ");
                            ex.printStackTrace(System.err);
                        }
                    }
                }

                System.out.println("Processed sectorfile " + inputFile.file.getAbsolutePath());
            }
        }

        executor.execute(new RegionFileReader());
    }

    public static void run(final String[] args) {
        final String inputDirectoryPath = System.getProperty(INPUT_PROPERTY);
        if (inputDirectoryPath == null) {
            System.err.println("Must specify base world directory as -D" + INPUT_PROPERTY + "=<path>");
            return;
        }
        final File inputDir = new File(inputDirectoryPath);
        if (!inputDir.isDirectory()) {
            System.err.println("Specified input is not a directory");
            return;
        }

        final int compressionType = Integer.getInteger(TYPE_OUTPUT_PROPERTY, -1);
        if (compressionType < 0 || compressionType > 4) {
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
            System.out.println("DeConverting dimension " + (subdir.isEmpty() ? "overworld" : subdir));

            final File dimDirectory = subdir.isEmpty() ? inputDir : new File(inputDir, subdir);

            final File sectorDir = new File(dimDirectory, SOURCE_DIRECTORY);

            if (!dimDirectory.isDirectory() || !sectorDir.isDirectory()) {
                System.out.println("Skipping dimension " + (subdir.isEmpty() ? "overworld" : subdir) + ", it is empty");
                continue;
            }

            final Set<String> toConvert = new LinkedHashSet<>();

            for (final String name : sectorDir.list((final File dir, final String name) -> {
                return name.endsWith(SectorFile.FILE_EXTENSION);
            })) {
                toConvert.add(name);
            }

            if (toConvert.isEmpty()) {
                System.out.println("No regions in " + sectorDir.getAbsolutePath());
                continue;
            }

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

            System.out.println("DeConverted dimension " + (subdir.isEmpty() ? "overworld" : subdir));
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
