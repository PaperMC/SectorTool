package ca.spottedleaf.regioncompresstest.test;

import ca.spottedleaf.io.buffer.BufferTracker;
import ca.spottedleaf.io.region.io.java.SimpleBufferedOutputStream;
import ca.spottedleaf.io.stream.databuffered.AbstractBufferedDataByteBufferInputStream;
import ca.spottedleaf.io.stream.databuffered.wrapped.WrappedBufferedDataByteBufferInputStream;
import ca.spottedleaf.io.stream.file.FileChannelByteBufferInputStream;
import ca.spottedleaf.io.buffer.BufferChoices;
import ca.spottedleaf.regioncompresstest.Main;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public final class ReadProfilingTest {

    private static final ThreadLocal<DecimalFormat> TWO_DECIMAL_PLACES = ThreadLocal.withInitial(() -> {
        return new DecimalFormat("#,##0.00");
    });
    private static final ThreadLocal<DecimalFormat> ONE_DECIMAL_PLACES = ThreadLocal.withInitial(() -> {
        return new DecimalFormat("#,##0.0");
    });
    private static final ThreadLocal<DecimalFormat> NO_DECIMAL_PLACES = ThreadLocal.withInitial(() -> {
        return new DecimalFormat("#,##0");
    });

    public static final String INPUT_PROPERTY = "input";

    private static AbstractBufferedDataByteBufferInputStream createInStream(final File file, final BufferTracker bufferTracker) {
        try {
            return new WrappedBufferedDataByteBufferInputStream(
                    new FileChannelByteBufferInputStream(file, StandardOpenOption.READ),
                    bufferTracker.acquireDirectBuffer()
            );
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static record SummaryStats(double p25, double median, double p75, double average, Histogram histogram,
                                       DoubleArrayList list) {}

    private static double getPercentile(final DoubleArrayList list, final double percentile) {
        return list.getDouble((int)Math.floor(percentile * list.size()));
    }

    private static double getAvgSorted(final DoubleArrayList list) {
        // best precision is when list is sorted from smallest magnitude->largest
        double sum = 0.0;
        for (final DoubleIterator iterator = list.iterator(); iterator.hasNext();) {
            sum += iterator.nextDouble();
        }

        return sum / (double)list.size();
    }

    private static record Histogram(
            double min, double max, double per,
            long[] counts
    ) { }

    private static Histogram createHistogram(final DoubleArrayList sorted, final int divisions) {
        final double min = sorted.getDouble(0);
        final double max = sorted.getDouble(sorted.size() - 1);

        final double per = (max - min) / (double)divisions;

        final double[] limits = new double[divisions];

        for (int i = 0; i < divisions; ++i) {
            limits[i] = (double)(i + 1) * per;
        }

        final long[] counts = new long[divisions];

        int hindex = 0;
        for (int i = 0; i < sorted.size(); ++i) {
            final double value = sorted.getDouble(i);

            while (hindex < (divisions - 1) && value > limits[hindex]) {
                ++hindex;
            }

            ++counts[hindex];
        }

        return new Histogram(
                min, max, per, counts
        );
    }

    private static SummaryStats getSummaryStats(final DoubleArrayList list, final int divisions) {
        final DoubleArrayList copy = list.clone();
        list.sort(null);


        return new SummaryStats(
                getPercentile(list, 0.25),
                getPercentile(list, 0.50),
                getPercentile(list, 0.75),
                getAvgSorted(list),
                createHistogram(list, divisions),
                copy
        );
    }

    private static record Summary(
            SummaryStats compressTimesUS, SummaryStats decompressTimesUS, SummaryStats compressRatio, SummaryStats compressSize,
            long totalBytes,
            long totalSectors
    ) {}

    private static Summary summary(final int index, final List<RunProfilingTest.TestResult> results, final int sectorSize,
                                   final int histogramDivisions) {
        final DoubleArrayList list = new DoubleArrayList(results.size());

        final double NS_TO_US = (1.0E3) / (1.0E6);

        for (final RunProfilingTest.TestResult result : results) {
            list.add((double)result.compressTimes()[index] * NS_TO_US);
        }

        final SummaryStats compressTimesUS = getSummaryStats(list, histogramDivisions);

        list.clear();

        for (final RunProfilingTest.TestResult result : results) {
            list.add((double)result.decompressTimes()[index] * NS_TO_US);
        }

        final SummaryStats decompressTimesUS = getSummaryStats(list, histogramDivisions);

        list.clear();

        for (final RunProfilingTest.TestResult result : results) {
            list.add((double)result.originalSizeBytes() / (double)result.compressedSizeBytes()[index]);
        }

        final SummaryStats compressRatio = getSummaryStats(list, histogramDivisions);

        list.clear();

        for (final RunProfilingTest.TestResult result : results) {
            list.add((double)result.compressedSizeBytes()[index]);
        }

        final SummaryStats compressedSize = getSummaryStats(list, histogramDivisions);

        list.clear();

        long totalSectors = 0;
        long totalBytes = 0;
        for (final RunProfilingTest.TestResult result : results) {
            final int compressedBytes = result.compressedSizeBytes()[index];
            totalSectors += (long)((compressedBytes + (sectorSize - 1)) / sectorSize);
            totalBytes += (long)compressedBytes;
        }

        return new Summary(compressTimesUS, decompressTimesUS, compressRatio, compressedSize, totalBytes, totalSectors);
    }

    private static String summarize(final SummaryStats stats, final String unit) {
        return "q25: " + TWO_DECIMAL_PLACES.get().format(stats.p25) + unit + ", median: " + TWO_DECIMAL_PLACES.get().format(stats.median) + unit + ", q75: " + TWO_DECIMAL_PLACES.get().format(stats.p75) + unit + ", average: " + TWO_DECIMAL_PLACES.get().format(stats.average) + unit;
    }

    private static final char DELIMITER = '|';

    private static void printHistogram(final PrintStream ps, final RunProfilingTest.TestResultPalette palette, final Summary[] datas) {
        /*
         * Name1\t
         * compress time (us)\tdecompress time (us)\tcompress ratio\t\t
         *
         *
         */

        final int rowsPerType = 4;
        final int buffer = 1;

        // write header
        for (final String name : palette.mapping()) {
            ps.print(name);
            for (int i = 0; i < rowsPerType + buffer; ++i) {
                ps.print(DELIMITER);
            }
        }
        ps.println();

        for (int k = 0; k < palette.mapping().length; ++k) {
            ps.print("compress time (us)"); ps.print(DELIMITER);
            ps.print("decompress time (us)"); ps.print(DELIMITER);
            ps.print("compress ratio"); ps.print(DELIMITER);
            ps.print("compressed size"); ps.print(DELIMITER);

            for (int i = 0; i < buffer; ++i) {
                ps.print(DELIMITER);
            }
        }

        ps.println();

        int maxLen = datas[0].compressTimesUS.list.size();

        for (int i = 0; i < maxLen; ++i) {
            for (int j = 0; j < palette.mapping().length; ++j) {
                ps.print(datas[j].compressTimesUS.list.getDouble(i)); ps.print(DELIMITER);
                ps.print(datas[j].decompressTimesUS.list.getDouble(i)); ps.print(DELIMITER);
                ps.print(datas[j].compressRatio.list.getDouble(i)); ps.print(DELIMITER);
                ps.print((long)datas[j].compressSize.list.getDouble(i)); ps.print(DELIMITER);

                for (int k = 0; k < buffer; ++k) {
                    ps.print(DELIMITER);
                }
            }

            ps.println();
        }
    }

    public static void run(final String[] args) {
        final String inputFilePath = System.getProperty(INPUT_PROPERTY);
        if (inputFilePath == null) {
            System.err.println("Must specify input as -D" + INPUT_PROPERTY + "=<path>");
            return;
        }
        final File inputFile = new File(inputFilePath);
        if (!inputFile.isFile()) {
            System.err.println("Specified input is not a file");
            return;
        }

        final BufferChoices bufferChoices = BufferChoices.createNew(Main.THREADS * 20);

        final AbstractBufferedDataByteBufferInputStream in = createInStream(inputFile, bufferChoices.scope().t1m());
        final List<RunProfilingTest.TestResult> results = new ArrayList<>();
        final RunProfilingTest.TestResultPalette palette;
        try {
            palette = RunProfilingTest.TestResultPalette.read(in);

            RunProfilingTest.TestResult result;
            while ((result = RunProfilingTest.TestResult.read(palette, in)) != null) {
                results.add(result);
            }
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            try {
                in.close();
            } catch (final IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        final int sectorSize = 512;
        final int divisions = 25;

        final List<Summary> datas = new ArrayList<>(palette.mapping().length);

        try {
            for (int i = 0, len = palette.mapping().length; i < len; ++i) {
                final String name = palette.mapping()[i];
                System.out.println("Computing stats for compression type: " + name);

                final Summary summary = summary(i, results, sectorSize, divisions);

                System.out.println("Compression times (us): " + summarize(summary.compressTimesUS, "us"));
                System.out.println("Decompression times (us): " + summarize(summary.decompressTimesUS, "us"));
                System.out.println("Compression ratio (old size / new size): " + summarize(summary.compressRatio, ""));
                System.out.println("Total sectors used (" + sectorSize + " bytes): " + summary.totalSectors);
                System.out.println("Total bytes: " + summary.totalBytes);
                System.out.println("Sector efficiency (total bytes / allocated sector bytes): " + TWO_DECIMAL_PLACES.get().format((double) summary.totalBytes / (double) (summary.totalSectors * sectorSize)));

                System.out.println();

                datas.add(summary);
            }

            final File psvFile = new File("results" + ".psv");
            psvFile.delete();
            psvFile.createNewFile();

            try (final PrintStream psv = new PrintStream(new SimpleBufferedOutputStream(new FileOutputStream(psvFile)), false, StandardCharsets.UTF_8)) {
                printHistogram(psv, palette, datas.toArray(new Summary[0]));
            }
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
