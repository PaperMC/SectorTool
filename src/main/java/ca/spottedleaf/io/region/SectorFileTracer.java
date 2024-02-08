package ca.spottedleaf.io.region;

import ca.spottedleaf.io.region.io.bytebuffer.BufferedFileChannelInputStream;
import ca.spottedleaf.io.region.io.bytebuffer.BufferedFileChannelOutputStream;
import ca.spottedleaf.io.region.io.java.SimpleBufferedInputStream;
import ca.spottedleaf.io.region.io.java.SimpleBufferedOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

public final class SectorFileTracer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SectorFileTracer.class);

    private final File file;
    private final DataOutputStream out;
    private final ArrayDeque<Writable> objects = new ArrayDeque<>();

    private static final int MAX_STORED_OBJECTS = 128;
    private static final TraceEventType[] EVENT_TYPES = TraceEventType.values();

    public SectorFileTracer(final File file) throws IOException {
        this.file = file;

        file.getParentFile().mkdirs();
        file.delete();
        file.createNewFile();

        final int bufferSize = 8 * 1024;

        this.out = new DataOutputStream(
            new SimpleBufferedOutputStream(
                new BufferedFileChannelOutputStream(ByteBuffer.allocateDirect(bufferSize), file.toPath(), true),
                new byte[bufferSize]
            )
        );
    }

    public synchronized void add(final Writable writable) {
        this.objects.add(writable);
        if (this.objects.size() >= MAX_STORED_OBJECTS) {
            Writable polled = null;
            try {
                while ((polled = this.objects.poll()) != null) {
                    polled.write(this.out);
                }
            } catch (final IOException ex) {
                LOGGER.error("Failed to write " + polled + ": ", ex);
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            Writable polled;
            while ((polled = this.objects.poll()) != null) {
                polled.write(this.out);
            }
        } finally {
            this.out.close();
        }
    }

    private static Writable read(final DataInputStream input) throws IOException {
        final int next = input.read();
        if (next == -1) {
            return null;
        }

        final TraceEventType event = EVENT_TYPES[next & 0xFF];

        switch (event) {
            case DATA: {
                return DataEvent.read(input);
            }
            case FILE: {
                return FileEvent.read(input);
            }
            default: {
                throw new IllegalStateException("Unknown event: " + event);
            }
        }
    }

    public static List<Writable> read(final File file) throws IOException {
        final List<Writable> ret = new ArrayList<>();

        final int bufferSize = 8 * 1024;

        try (final DataInputStream is = new DataInputStream(
            new SimpleBufferedInputStream(
                new BufferedFileChannelInputStream(ByteBuffer.allocateDirect(bufferSize), file),
                new byte[bufferSize]
            )
        )) {
            Writable curr;
            while ((curr = read(is)) != null) {
                ret.add(curr);
            }

            return ret;
        }
    }

    public static interface Writable {
        public void write(final DataOutput out) throws IOException;
    }

    public static enum TraceEventType {
        FILE, DATA;
    }

    public static enum FileEventType {
        CREATE, OPEN, CLOSE;
    }

    public static record FileEvent(
        FileEventType eventType, int sectionX, int sectionZ
    ) implements Writable {
        private static final FileEventType[] TYPES = FileEventType.values();

        @Override
        public void write(final DataOutput out) throws IOException {
            out.writeByte(TraceEventType.FILE.ordinal());
            out.writeByte(this.eventType().ordinal());
            out.writeInt(this.sectionX());
            out.writeInt(this.sectionZ());
        }

        public static FileEvent read(final DataInput input) throws IOException {
            return new FileEvent(
                TYPES[(int)input.readByte() & 0xFF],
                input.readInt(),
                input.readInt()
            );
        }
    }

    public static enum DataEventType {
        READ, WRITE, DELETE;
    }

    public static record DataEvent(
        DataEventType eventType, int chunkX, int chunkZ, byte type, int size
    ) implements Writable {

        private static final DataEventType[] TYPES = DataEventType.values();

        @Override
        public void write(final DataOutput out) throws IOException {
            out.writeByte(TraceEventType.DATA.ordinal());
            out.writeByte(this.eventType().ordinal());
            out.writeInt(this.chunkX());
            out.writeInt(this.chunkZ());
            out.writeByte(this.type());
            out.writeInt(this.size());
        }

        public static DataEvent read(final DataInput input) throws IOException {
            return new DataEvent(
                TYPES[(int)input.readByte() & 0xFF],
                input.readInt(),
                input.readInt(),
                input.readByte(),
                input.readInt()
            );
        }
    }
}
