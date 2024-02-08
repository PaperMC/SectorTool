package ca.spottedleaf.io.region.io.bytebuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class BufferedFileChannelOutputStream extends ByteBufferOutputStream {

    private final FileChannel channel;

    public BufferedFileChannelOutputStream(final ByteBuffer buffer, final Path path, final boolean append) throws IOException {
        super(buffer);

        if (append) {
            this.channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } else {
            this.channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        }
    }

    @Override
    protected ByteBuffer flush(final ByteBuffer current) throws IOException {
        current.flip();

        while (current.hasRemaining()) {
            this.channel.write(current);
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
            this.channel.close();
        }
    }
}
