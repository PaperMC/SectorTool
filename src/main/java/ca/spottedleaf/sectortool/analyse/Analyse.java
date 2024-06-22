package ca.spottedleaf.sectortool.analyse;

import ca.spottedleaf.io.buffer.BufferChoices;
import ca.spottedleaf.io.region.MinecraftRegionFileType;
import ca.spottedleaf.io.region.SectorFile;
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

public final class Analyse {

    private static final String[] SUBDIRS = new String[] {
            "",
            "DIM-1",
            "DIM1"
    };

    public static final String INPUT_PROPERTY = "input";

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

    private static class StatsAccumulator {

        public long fileSectors = 0L;
        public long allocatedSectors = 0L;
        public long alternateAllocatedSectors = 0L;
        public long alternateAllocatedSectorsPadded = 0L;
        public long dataSizeBytes = 0L;
        public long errors = 0L;

        public synchronized void accumulate(final RegionFile.AllocationStats stats) {
            this.fileSectors += stats.fileSectors();
            this.allocatedSectors += stats.allocatedSectors();
            this.alternateAllocatedSectors += stats.alternateAllocatedSectors();
            this.alternateAllocatedSectorsPadded += stats.alternateAllocatedSectorsPadded();
            this.dataSizeBytes += stats.dataSizeBytes();
            this.errors += (long)stats.errors();
        }

        public void print() {
            System.out.println("File sectors: " + this.fileSectors);
            System.out.println("Allocated sectors: " + this.allocatedSectors);
            System.out.println("Alternate allocated sectors: " + this.alternateAllocatedSectors);
            System.out.println("Alternate allocated sectors padded: " + this.alternateAllocatedSectorsPadded);
            System.out.println("Total data size: " + this.dataSizeBytes);
            System.out.println("Errors: " + this.errors);
        }
    }

    private static void submitToExecutor(final ExecutorService executor, final File dimDirectory, final String regionName,
                                         final BufferChoices unscopedBufferChoices, final StatsAccumulator accumulator,
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
                try {
                    for (final MinecraftRegionFileType type : MinecraftRegionFileType.getAll()) {
                        final RegionFile regionFile = byId.get(type.getNewId());
                        if (regionFile == null) {
                            continue;
                        }

                        final RegionFile.AllocationStats stats;
                        try {
                            stats = regionFile.computeStats(unscopedBufferChoices, SectorFile.SECTOR_SIZE,
                                    SectorFile.TYPE_HEADER_SECTORS + SectorFile.FileHeader.FILE_HEADER_TOTAL_SECTORS,
                                    SectorFile.DataHeader.DATA_HEADER_LENGTH);
                        } catch (final IOException ex) {
                            synchronized (System.err) {
                                System.err.println("Failed to read stats from regionfile '" + regionFile.file.getAbsolutePath() + "': ");
                                ex.printStackTrace(System.err);
                            }
                            continue;
                        }

                        accumulator.accumulate(stats);
                    }
                } finally {
                    decrementTracker(concurrentTracker, wakeup, threshold);

                    for (final RegionFile regionFile : byId.values()) {
                        System.out.println("Processed regionfile " + regionFile.file.getAbsolutePath());
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

        final BufferChoices bufferChoices = BufferChoices.createNew(Main.THREADS * 20);

        final ExecutorService executors = createExecutors();

        final AtomicInteger concurrentExecutions = new AtomicInteger();
        // avoid opening too many files by limiting the number of concurrently executing tasks
        final int targetConcurrent = Main.THREADS * 4;
        final int waitThreshold = targetConcurrent / 2;

        final StatsAccumulator accumulator = new StatsAccumulator();

        for (final String subdir : SUBDIRS) {
            System.out.println("Analysing dimension " + (subdir.isEmpty() ? "overworld" : subdir));

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

            for (final String convert : toConvert) {
                submitToExecutor(executors, dimDirectory, convert, bufferChoices, accumulator, concurrentExecutions, Thread.currentThread(), waitThreshold);

                if (concurrentExecutions.get() >= targetConcurrent) {
                    while (concurrentExecutions.get() > waitThreshold) {
                        LockSupport.park("Awaiting for max concurrent");
                    }
                }
            }

            while (concurrentExecutions.get() > 0) {
                LockSupport.park("Awaiting finish");
            }

            System.out.println("Analysed dimension " + (subdir.isEmpty() ? "overworld" : subdir));
        }

        while (concurrentExecutions.get() > 0) {
            LockSupport.park("Awaiting finish");
        }

        executors.shutdown();
        try {
            executors.awaitTermination(1, TimeUnit.MINUTES);
        } catch (final InterruptedException ignore) {}

        accumulator.print();
    }
}
