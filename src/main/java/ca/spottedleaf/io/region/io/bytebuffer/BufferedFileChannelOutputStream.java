package ca.spottedleaf.io.region.io.bytebuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;

public class BufferedFileChannelOutputStream extends ByteBufferOutputStream {

    private final FileChannel output;

    public BufferedFileChannelOutputStream(final ByteBuffer buffer, final File file, final OpenOption... openOptions) throws IOException {
        this(buffer, file.toPath());
    }

    public BufferedFileChannelOutputStream(final ByteBuffer buffer, final Path path, final OpenOption... openOptions) throws IOException {
        super(buffer);

        this.output = FileChannel.open(path, openOptions);

        // ensure we can fully utilise the buffer
        buffer.limit(buffer.capacity());
        buffer.position(0);
    }

    @Override
    protected ByteBuffer flush(final ByteBuffer current) throws IOException {
        current.flip();

        while (current.hasRemaining()) {
            this.output.write(current);
        }

        current.limit(current.capacity());
        current.position(0);

        return current;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            this.output.close();
        }
    }
}
