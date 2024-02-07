package ca.spottedleaf.regioncompresstest.test;

import ca.spottedleaf.io.buffer.BufferTracker;
import ca.spottedleaf.io.buffer.ZstdCtxManager;
import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferInputStream;
import ca.spottedleaf.io.region.io.bytebuffer.ByteBufferOutputStream;
import ca.spottedleaf.io.region.io.zstd.ZSTDInputStream;
import ca.spottedleaf.io.region.io.zstd.ZSTDOutputStream;
import ca.spottedleaf.regioncompresstest.stream.compat.FromJavaInputStream;
import ca.spottedleaf.regioncompresstest.stream.compat.FromJavaOutputStream;
import ca.spottedleaf.regioncompresstest.stream.databuffered.AbstractBufferedDataByteBufferInputStream;
import ca.spottedleaf.regioncompresstest.stream.databuffered.AbstractBufferedDataByteBufferOutputStream;
import ca.spottedleaf.regioncompresstest.stream.databuffered.wrapped.WrappedBufferedDataByteBufferOutputStream;
import ca.spottedleaf.regioncompresstest.stream.databuffered.zstd.wrapped.WrappedZSTDBufferedDataByteBufferInputStream;
import ca.spottedleaf.regioncompresstest.stream.databuffered.zstd.wrapped.WrappedZSTDBufferedDataByteBufferOutputStream;
import ca.spottedleaf.regioncompresstest.stream.file.FileChannelByteBufferOutputStream;
import ca.spottedleaf.io.buffer.BufferChoices;
import ca.spottedleaf.regioncompresstest.Main;
import ca.spottedleaf.regioncompresstest.storage.RegionFile;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

public final class RunProfilingTest {

    public static final String INPUT_PROPERTY = "input";
    public static final String OUTPUT_PROPERTY = "output";
    public static final String MAX_REGIONS_PROPERTY = "max_regions";

    private static ExecutorService createExecutors() {
        return Executors.newFixedThreadPool(Main.THREADS, new ThreadFactory() {
            private final AtomicInteger idGenerator = new AtomicInteger();

            @Override
            public Thread newThread(final Runnable run) {
                final Thread ret = new Thread(run);

                ret.setName("Profile worker #" + this.idGenerator.getAndIncrement());
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

    private static AbstractBufferedDataByteBufferOutputStream createOutStream(final File file, final BufferTracker bufferTracker) {
        try {
            return new WrappedBufferedDataByteBufferOutputStream(
                new FileChannelByteBufferOutputStream(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW),
                bufferTracker.acquireDirectBuffer()
            );
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void decrementTracker(final AtomicInteger concurrentTracker, final Thread wakeup, final int threshold) {
        final int count = concurrentTracker.decrementAndGet();
        if (count == threshold || count == 0) {
            LockSupport.unpark(wakeup);
        }
    }

    public static enum CompressionType {
        GZIP("gzip") {
            @Override
            public void compress(final BufferTracker bufferTracker, final ZstdCtxManager zstd, final byte[] data, final int off, final int len, final ByteArrayOutputStream vout) throws IOException {
                final GZIPOutputStream out = new GZIPOutputStream(vout);

                try {
                    out.write(data, off, len);
                } finally {
                    out.close();
                }
            }

            @Override
            public int decompress(final BufferTracker bufferTracker, final ZstdCtxManager zstd, final byte[] data, final int off, final int len) throws IOException {
                final GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(data, off, len));
                try {
                    final byte[] tmp = bufferTracker.acquireJavaBuffer();

                    int size = 0;

                    int r;
                    while ((r = in.read(tmp, 0, tmp.length)) >= 0) {
                        if (r == 0) {
                            throw new IllegalStateException();
                        }
                        size += r;
                    }

                    return size;
                } finally {
                    in.close();
                }
            }
        },
        DEFLATE("deflate") {
            @Override
            public void compress(final BufferTracker bufferTracker, final ZstdCtxManager zstd, final byte[] data, final int off, final int len, final ByteArrayOutputStream vout) throws IOException {
                final DeflaterOutputStream out = new DeflaterOutputStream(vout);

                try {
                    out.write(data, off, len);
                } finally {
                    out.close();
                }
            }

            @Override
            public int decompress(final BufferTracker bufferTracker, final ZstdCtxManager zstd, final byte[] data, final int off, final int len) throws IOException {
                final InflaterInputStream in = new InflaterInputStream(new ByteArrayInputStream(data, off, len));
                try {
                    final byte[] tmp = bufferTracker.acquireJavaBuffer();

                    int size = 0;

                    int r;
                    while ((r = in.read(tmp, 0, tmp.length)) >= 0) {
                        if (r == 0) {
                            throw new IllegalStateException();
                        }
                        size += r;
                    }
                    return size;
                } finally {
                    in.close();
                }
            }
        },
        LZ4("lz4") {
            @Override
            public void compress(final BufferTracker bufferTracker, final ZstdCtxManager zstd, final byte[] data, final int off, final int len, final ByteArrayOutputStream vout) throws IOException {
                final LZ4BlockOutputStream out = new LZ4BlockOutputStream(vout);

                try {
                    out.write(data, off, len);
                } finally {
                    out.close();
                }
            }

            @Override
            public int decompress(final BufferTracker bufferTracker, final ZstdCtxManager zstd, final byte[] data, final int off, final int len) throws IOException {
                final LZ4BlockInputStream in = new LZ4BlockInputStream(new ByteArrayInputStream(data, off, len));
                try {
                    final byte[] tmp = bufferTracker.acquireJavaBuffer();

                    int size = 0;

                    int r;
                    while ((r = in.read(tmp, 0, tmp.length)) >= 0) {
                        if (r == 0) {
                            throw new IllegalStateException();
                        }
                        size += r;
                    }

                    return size;
                } finally {
                    in.close();
                }
            }
        },
        LEAF_ZSTD("leaf-zstd") {
            @Override
            public void compress(final BufferTracker bufferTracker, final ZstdCtxManager zstd, final byte[] data, final int off, final int len, final ByteArrayOutputStream vout) throws IOException {
                final WrappedZSTDBufferedDataByteBufferOutputStream out = new WrappedZSTDBufferedDataByteBufferOutputStream(
                    bufferTracker.acquireDirectBuffer(), zstd.acquireCompress(),
                    new FromJavaOutputStream(
                        vout, bufferTracker.acquireJavaBuffer()
                    ),
                    bufferTracker.acquireDirectBuffer(),
                    zstd::returnCompress
                );

                try {
                    out.write(data, off, len);
                } finally {
                    out.close();
                }
            }

            @Override
            public int decompress(final BufferTracker bufferTracker, final ZstdCtxManager zstd, final byte[] data, final int off, final int len) throws IOException {
                final WrappedZSTDBufferedDataByteBufferInputStream in = new WrappedZSTDBufferedDataByteBufferInputStream(
                        bufferTracker.acquireDirectBuffer(), zstd.acquireDecompress(),
                        new FromJavaInputStream(
                                new ByteArrayInputStream(data, off, len),
                                bufferTracker.acquireJavaBuffer()
                        ),
                        bufferTracker.acquireDirectBuffer(),
                        zstd::returnDecompress
                );
                try {
                    final ByteBuffer tmp = bufferTracker.acquireDirectBuffer();

                    int size = 0;

                    int r;
                    while ((r = in.read(tmp, 0, tmp.capacity())) >= 0) {
                        if (r == 0) {
                            throw new IllegalStateException();
                        }
                        size += r;
                    }

                    return size;
                } finally {
                    in.close();
                }
            }
        },
        LEAF2_ZSTD("leaf2-zstd") {
            @Override
            public void compress(final BufferTracker bufferTracker, final ZstdCtxManager zstd, final byte[] data, final int off, final int len,
                                 final ByteArrayOutputStream vout) throws IOException {
                final ZSTDOutputStream out = new ZSTDOutputStream(
                        bufferTracker.acquireDirectBuffer(), bufferTracker.acquireDirectBuffer(),
                        zstd.acquireCompress(), zstd::returnCompress,
                        /* This looks bad, but SectorFile always provides a ByteBufferOutputStream as a parameter, so no hacks are needed in practice */
                        new ByteBufferOutputStream(ByteBuffer.wrap(bufferTracker.acquireJavaBuffer())) {
                            @Override
                            protected ByteBuffer flush(final ByteBuffer current) throws IOException {
                                vout.write(current.array(), 0, current.position());

                                current.limit(current.capacity());
                                current.position(0);

                                return current;
                            }
                        }
                );

                try {
                    out.write(data, off, len);
                } finally {
                    out.close();
                }
            }

            @Override
            public int decompress(final BufferTracker bufferTracker, final ZstdCtxManager zstd, final byte[] data, final int off, final int len) throws IOException {
                final ZSTDInputStream in = new ZSTDInputStream(
                        bufferTracker.acquireDirectBuffer(), bufferTracker.acquireDirectBuffer(), zstd.acquireDecompress(),
                        zstd::returnDecompress,
                        /* This looks bad, but SectorFile always provides a ByteBufferInputStream as a parameter, so no hacks are needed in practice */
                        new ByteBufferInputStream(ByteBuffer.wrap(data, off, len))
                );

                try {
                    final byte[] tmp = bufferTracker.acquireJavaBuffer();

                    int size = 0;

                    int r;
                    while ((r = in.read(tmp, 0, tmp.length)) >= 0) {
                        if (r == 0) {
                            throw new IllegalStateException();
                        }
                        size += r;
                    }

                    return size;
                } finally {
                    in.close();
                }
            }
        };

        public final String friendlyName;

        private CompressionType(final String friendlyName) {
            this.friendlyName = friendlyName;
        }

        public abstract void compress(final BufferTracker bufferTracker, final ZstdCtxManager zstd, final byte[] data, final int off, final int len, final ByteArrayOutputStream vout) throws IOException;

        public abstract int decompress(final BufferTracker bufferTracker, final ZstdCtxManager zstd, final byte[] data, final int off, final int len) throws IOException;

        public static TestResultPalette getPalette() {
            final CompressionType[] types = values();
            final String[] mapping = new String[types.length];
            for (int i = 0; i < types.length; ++i) {
                mapping[i] = types[i].friendlyName;
            }

            return new TestResultPalette(mapping);
        }

        public static TestResult doTest(final BufferTracker tracker1m0, final BufferTracker tracker0, final ZstdCtxManager zstd, final byte[] data, final int off, final int len) {
            final CompressionType[] types = values();

            final long[] compressTimes = new long[types.length];
            final long[] decompressTimes = new long[types.length];
            final int originalSizeBytes = len;
            final int[] compressedSizeBytes = new int[types.length];

            try (final BufferTracker tracker1m = tracker1m0.scope()) {
                final byte[] buffer = tracker1m.acquireJavaBuffer();
                final RegionFile.CustomByteArrayOutputStream out = new RegionFile.CustomByteArrayOutputStream(buffer);

                for (int i = 0; i < types.length; ++i) {
                    final CompressionType type = types[i];

                    out.reset();

                    final long compressStart = System.nanoTime();
                    try (final BufferTracker tracker = tracker0.scope()) {
                        type.compress(tracker, zstd, data, off, len, out);
                    } catch (final IOException ex) {
                        throw new RuntimeException(ex);
                    }
                    final long compressEnd = System.nanoTime();
                    final int compressedSize = out.size();

                    final long decompressStart = System.nanoTime();
                    try (final BufferTracker tracker = tracker0.scope()) {
                        final int decompressedSize = type.decompress(tracker, zstd, out.getBuffer(), 0, out.size());
                        if (decompressedSize != len) {
                            throw new IllegalStateException("Type " + type + " failed to produce same output");
                        }
                    } catch (final IOException ex) {
                        throw new RuntimeException(ex);
                    }
                    final long decompressEnd = System.nanoTime();

                    compressTimes[i] = compressEnd - compressStart;
                    decompressTimes[i] = decompressEnd - decompressStart;
                    compressedSizeBytes[i] = compressedSize;
                }
            }

            return new TestResult(compressTimes, decompressTimes, originalSizeBytes, compressedSizeBytes);
        }
    }

    private static final byte TEST_NO_MORE = (byte)0;
    private static final byte TEST_MORE = (byte)1;
    private static final byte TEST_PALETTE = (byte)2;

    public static record TestResultPalette(String[] mapping) {
        public static TestResultPalette read(final AbstractBufferedDataByteBufferInputStream in) throws IOException {
            final byte identifier = in.readByte();
            if (identifier != TEST_PALETTE) {
                throw new IllegalStateException("Expected palette identifier, got " + identifier);
            }
            final int len = in.readInt();
            final String[] mapping = new String[len];

            for (int i = 0; i < len; ++i) {
                mapping[i] = in.readUTF();
            }

            return new TestResultPalette(mapping);
        }

        public void write(final AbstractBufferedDataByteBufferOutputStream out) throws IOException {
            out.writeByte(TEST_PALETTE);
            out.writeInt(this.mapping.length);
            for (final String str : this.mapping) {
                out.writeUTF(str);
            }
        }
    }

    public static record TestResult(
            long[] compressTimes,
            long[] decompressTimes,
            int originalSizeBytes,
            int[] compressedSizeBytes
    ) {
        public static TestResult read(final TestResultPalette palette, final AbstractBufferedDataByteBufferInputStream in) throws IOException {
            final byte identifier = in.readByte();

            if (identifier == TEST_NO_MORE) {
                return null;
            } else if (identifier != TEST_MORE) {
                throw new IllegalArgumentException("Expected identifier MORE, but got " + identifier);
            }

            final int len = palette.mapping.length;

            final long[] compressTimes = new long[len];
            final long[] decompressTimes = new long[len];
            final int originalSizeBytes;
            final int[] compressedSizeBytes = new int[len];

            in.readFully(compressTimes);
            in.readFully(decompressTimes);
            originalSizeBytes = in.readInt();
            in.readFully(compressedSizeBytes);

            return new TestResult(compressTimes, decompressTimes, originalSizeBytes, compressedSizeBytes);
        }

        public void write(final AbstractBufferedDataByteBufferOutputStream out) throws IOException {
            out.writeByte(TEST_MORE);
            out.write(this.compressTimes());
            out.write(this.decompressTimes());
            out.writeInt(this.originalSizeBytes());
            out.write(this.compressedSizeBytes());
        }
    }

    private static void submitToExecutor(final ExecutorService executor, final File file, final BufferChoices unscopedBufferChoices,
                                         final AbstractBufferedDataByteBufferOutputStream out,
                                         final AtomicInteger concurrentTracker, final Thread wakeup, final int threshold) {
        final String regionName = file.getName();
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

        final RegionFile regionFile;
        try  {
            regionFile = new RegionFile(file, sectionX, sectionZ, unscopedBufferChoices, true);
        } catch (final IOException ex) {
            synchronized (System.err) {
                System.err.println("Failed to open regionfile " + file.getAbsolutePath() + ": ");
                ex.printStackTrace(System.err);
            }

            return;
        }

        concurrentTracker.getAndIncrement();

        class RegionFileReader implements Runnable {

            @Override
            public void run() {
                final List<TestResult> results = new ArrayList<>();

                try (final BufferChoices scopedBufferChoices = unscopedBufferChoices.scope()) {
                    final RegionFile.CustomByteArrayOutputStream decompressed = new RegionFile.CustomByteArrayOutputStream(scopedBufferChoices.t1m().acquireJavaBuffer());

                    for (int i = 0; i < 32 * 32; ++i) {
                        final int chunkX = (i & 31);
                        final int chunkZ = ((i >>> 5) & 31);

                        decompressed.reset();
                        boolean read = false;

                        try (final BufferChoices readChoices = scopedBufferChoices.scope()) {
                            try {
                                read = regionFile.read(chunkX, chunkZ, readChoices, decompressed);
                            } catch (final IOException ex) {
                                synchronized (System.err) {
                                    System.err.println(
                                            "Failed to read chunk (" + chunkX + "," + chunkZ + ") from regionfile " +
                                                    regionFile.file.getAbsolutePath() + ": ");
                                    ex.printStackTrace(System.err);
                                }
                            }
                        }

                        if (!read) {
                            continue;
                        }

                        try (final BufferChoices testChoices = scopedBufferChoices.scope()) {
                            results.add(
                                    CompressionType.doTest(testChoices.t1m(), testChoices.t16k(), scopedBufferChoices.zstdCtxs().zstdCtxManager, decompressed.getBuffer(), 0, decompressed.size())
                            );
                        }
                    }
                } finally {
                    decrementTracker(concurrentTracker, wakeup, threshold);

                    try {
                        regionFile.close();
                    } catch (final IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }

                System.out.println("Processed regionfile " + regionFile.file.getAbsolutePath());

                if (!results.isEmpty()) {
                    synchronized (out) {
                        for (final TestResult result : results) {
                            try {
                                result.write(out);
                            } catch (final IOException ex) {
                                throw new RuntimeException(ex);
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
            System.err.println("Must specify input (directory or region file) as -D" + INPUT_PROPERTY + "=<path>");
            return;
        }
        final File inputDir = new File(inputDirectoryPath);
        if (!inputDir.isDirectory() && (!inputDir.isFile() || (!inputDirectoryPath.endsWith(RegionFile.ANVIL_EXTENSION) && !inputDirectoryPath.endsWith(RegionFile.MCREGION_EXTENSION)))) {
            System.err.println("Specified input is not a directory or .mca file");
            return;
        }

        final String outputFilePath = System.getProperty(OUTPUT_PROPERTY);
        if (outputFilePath == null) {
            System.err.println("Must specify output file as -D" + OUTPUT_PROPERTY + "=<path>");
            return;
        }

        final File ouputFile = new File(outputFilePath);
        if (ouputFile.exists()) {
            System.err.println("Output file already exists");
            return;
        }

        final int maxRegions = Integer.getInteger(MAX_REGIONS_PROPERTY, Integer.MAX_VALUE);

        final BufferChoices bufferChoices = BufferChoices.createNew(Main.THREADS * 20);

        final ExecutorService executors = createExecutors();
        final AbstractBufferedDataByteBufferOutputStream output = createOutStream(ouputFile, bufferChoices.scope().t1m());

        try {
            CompressionType.getPalette().write(output);
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }

        final AtomicInteger concurrentExecutions = new AtomicInteger();
        // avoid opening too many files by limiting the number of concurrently executing tasks
        final int targetConcurrent = Main.THREADS * 4;
        final int waitThreshold = targetConcurrent / 2;

        int regions = 0;

        if (inputDir.isFile()) {
            submitToExecutor(executors, inputDir, bufferChoices, output, concurrentExecutions, Thread.currentThread(), waitThreshold);
        } else {
            for (final File file : inputDir.listFiles((final File file) -> {
                final String name = file.getName();
                return name.endsWith(RegionFile.MCREGION_EXTENSION) || name.endsWith(RegionFile.ANVIL_EXTENSION);
            })) {
                submitToExecutor(executors, file, bufferChoices, output, concurrentExecutions, Thread.currentThread(), waitThreshold);

                if (++regions >= maxRegions) {
                    break;
                }

                if (concurrentExecutions.get() >= targetConcurrent) {
                    while (concurrentExecutions.get() > waitThreshold) {
                        LockSupport.park("Awaiting for max concurrent");
                    }
                }
            }
        }

        while (concurrentExecutions.get() > 0) {
            LockSupport.park("Awaiting finish");
        }

        executors.shutdown();
        try {
            executors.awaitTermination(1, TimeUnit.MINUTES);
        } catch (final InterruptedException ignore) {
        }

        try {
            synchronized (output) {
                try {
                    output.writeByte(TEST_NO_MORE);
                } finally {
                    output.close();
                }
            }
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
