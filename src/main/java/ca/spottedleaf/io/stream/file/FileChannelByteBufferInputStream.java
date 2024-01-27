package ca.spottedleaf.io.stream.file;

import ca.spottedleaf.io.stream.ByteBufferInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;

public class FileChannelByteBufferInputStream extends ByteBufferInputStream {

    private FileChannel channel;

    public FileChannelByteBufferInputStream(final File file, final OpenOption... options) throws IOException {
        this.channel = FileChannel.open(file.toPath(), options);
    }

    public FileChannel getChannel() {
        return this.channel;
    }

    @Override
    public int read() throws IOException {
        // file channel ops will cache a direct buffer to use, so we don't care about this non-direct allocation
        final ByteBuffer buffer = ByteBuffer.allocate(1);

        final int read = this.read(buffer);
        return read <= 0 ? -1 : (int)buffer.get(0) & 0xFF;
    }

    @Override
    public int read(ByteBuffer into, int offset, int length) throws IOException {
        if (((length | offset) | (offset + length) | (into.limit() - (offset + length))) < 0) {
            // length < 0 || off < 0 || (off + len) < 0
            throw new IndexOutOfBoundsException();
        }

        into = into.duplicate();

        into.limit(offset + length);
        into.position(offset);

        return this.channel.read(into);
    }

    @Override
    public int readFully(final ByteBuffer into, int offset, int length) throws IOException {
        if (((length | offset) | (offset + length) | (into.limit() - (offset + length))) < 0) {
            // length < 0 || off < 0 || (off + len) < 0
            throw new IndexOutOfBoundsException();
        }

        int total = 0;
        while (length > 0) {
            final int read = this.read(into, offset, length);
            if (read < 0) {
                throw new EOFException();
            }

            total += read;
            offset += read;
            length -= read;
        }

        return total;
    }

    @Override
    public long skip(final long n) throws IOException {
        if (this.channel == null) {
            throw new IOException("Closed stream");
        }

        final long position = this.channel.position();
        final long size = this.channel.size();

        final long toSkip = Math.min(Math.max(0L, size - position), n);

        if (toSkip != 0L) {
            this.channel.position(toSkip + position);
        }

        return toSkip;
    }

    @Override
    public void skipFully(final long n) throws IOException {
        if (this.channel == null) {
            throw new IOException("Closed stream");
        }

        final long position = this.channel.position();
        final long size = this.channel.size();

        final long toSkip = Math.min(Math.max(0L, size - position), n);

        if (n != toSkip) {
            throw new EOFException();
        }

        if (toSkip != 0L) {
            this.channel.position(toSkip + position);
        }
    }

    @Override
    public long available() throws IOException {
        if (this.channel == null) {
            throw new IOException("Closed stream");
        }

        final long position = this.channel.position();
        final long size = this.channel.size();

        return Math.max(0L, size - position);
    }

    @Override
    public void close() throws IOException {
        final FileChannel channel = this.channel;
        if (channel == null) {
            return;
        }

        this.channel = null;
        channel.close();
    }
}
