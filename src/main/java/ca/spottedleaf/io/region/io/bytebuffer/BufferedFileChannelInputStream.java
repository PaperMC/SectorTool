package ca.spottedleaf.io.region.io.bytebuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class BufferedFileChannelInputStream extends ByteBufferInputStream {

    protected final FileChannel input;

    public BufferedFileChannelInputStream(final ByteBuffer buffer, final File file) throws IOException {
        this(buffer, file.toPath());
    }

    public BufferedFileChannelInputStream(final ByteBuffer buffer, final Path path) throws IOException {
        super(buffer);

        this.input = FileChannel.open(path, StandardOpenOption.READ);

        // ensure we can fully utilise the buffer
        buffer.limit(buffer.capacity());
        buffer.position(buffer.capacity());
    }

    @Override
    public int available() throws IOException {
        final long avail = (long)super.available() + (this.input.size() - this.input.position());

        final int ret;
        if (avail < 0) {
            ret = 0;
        } else if (avail > (long)Integer.MAX_VALUE) {
            ret = Integer.MAX_VALUE;
        } else {
            ret = (int)avail;
        }

        return ret;
    }

    @Override
    protected ByteBuffer refill(final ByteBuffer current) throws IOException {
        // note: limit = capacity
        current.flip();

        this.input.read(current);

        current.flip();

        return current;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            // force any read calls to go to refill()
            this.buffer.limit(this.buffer.capacity());
            this.buffer.position(this.buffer.capacity());

            this.input.close();
        }
    }
}
