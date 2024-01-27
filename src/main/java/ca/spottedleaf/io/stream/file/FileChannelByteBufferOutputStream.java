package ca.spottedleaf.io.stream.file;

import ca.spottedleaf.io.stream.ByteBufferOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;

public class FileChannelByteBufferOutputStream extends ByteBufferOutputStream {

    private FileChannel channel;

    public FileChannelByteBufferOutputStream(final File file, final OpenOption... options) throws IOException {
        this.channel = FileChannel.open(file.toPath(), options);
    }

    public FileChannelByteBufferOutputStream(final Path path, final OpenOption... options) throws IOException {
        this.channel = FileChannel.open(path, options);
    }

    public FileChannel getChannel() {
        return this.channel;
    }

    @Override
    public void write(final int b) throws IOException {
        final FileChannel channel = this.channel;
        if (channel == null) {
            throw new IOException("Closed stream");
        }

        final ByteBuffer buffer = ByteBuffer.allocate(1);
        // file channel ops will cache a direct buffer to use, so we don't care about this non-direct allocation
        buffer.put((byte)b);

        while (channel.write(buffer) == 0);
    }

    @Override
    public void write(ByteBuffer src, int offset, int length) throws IOException {
        if (((length | offset) | (offset + length) | (src.limit() - (offset + length))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final FileChannel channel = this.channel;
        if (channel == null) {
            throw new IOException("Closed stream");
        }

        // do not clobber position/limit as specified
        src = src.duplicate();

        src.limit(offset + length);
        src.position(offset);

        int written = 0;

        while (written < length) {
            written += channel.write(src);
        }
    }

    @Override
    public void flush() throws IOException {
        if (this.channel == null) {
            throw new IOException("Closed stream");
        }
    }

    @Override
    public void close() throws IOException {
        final FileChannel channel = this.channel;
        if (channel == null) {
            return;
        }

        try {
            this.flush();
        } finally {
            this.channel = null;
            channel.close();
        }
    }
}
