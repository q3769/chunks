/*
 * MIT License
 *
 * Copyright (c) 2021 Qingtian Wang
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package chunk4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import javax.annotation.concurrent.ThreadSafe;
import lombok.NonNull;

/**
 * The ChunkChopper class is responsible for chopping data into chunks. It is thread-safe and provides a static factory
 * method for creating new instances. Each instance is configured with a maximum chunk byte size.
 *
 * @author Qingtian Wang
 */
@ThreadSafe
public final class ChunkChopper implements Chopper {

    private final int chunkCapacity;

    /**
     * Private constructor for the ChunkChopper class. It is used by the static factory method to create a new instance
     * of ChunkChopper.
     *
     * @param chunkByteCapacity The maximum size of the byte array in a chunk.
     */
    private ChunkChopper(int chunkByteCapacity) {
        if (chunkByteCapacity <= 0) {
            throw new IllegalArgumentException(
                    "Max size of the byte array in a chunk has to be a positive int: " + chunkByteCapacity);
        }
        this.chunkCapacity = chunkByteCapacity;
    }

    /**
     * Static factory method for creating a new ChunkChopper.
     *
     * @param maxChunkByteSize The maximum size of the byte array in a chunk.
     * @return A new ChunkChopper.
     */
    public static @NonNull ChunkChopper ofByteSize(int maxChunkByteSize) {
        return new ChunkChopper(maxChunkByteSize);
    }

    /**
     * Chops a byte array into chunks. Each chunk is represented by a Chunk object and contains a portion of the
     * original byte array. The chunks are added to a list, which is returned by the method.
     *
     * @param bytes The byte array to chop into chunks.
     * @return A list of chunks.
     */
    @Override
    public @NonNull List<Chunk> chop(byte[] bytes) {
        final List<Chunk> chunks = new ArrayList<>();
        final UUID groupId = UUID.randomUUID();
        final int groupSize = numberOfChunks(bytes);
        int chunkIndex = 0;
        for (int chunkBytesStart = 0; chunkBytesStart < bytes.length; chunkBytesStart += this.chunkCapacity) {
            int chunkBytesEnd = Math.min(bytes.length, chunkBytesStart + this.chunkCapacity);
            final byte[] chunkBytes = Arrays.copyOfRange(bytes, chunkBytesStart, chunkBytesEnd);
            chunks.add(Chunk.builder()
                    .groupId(groupId)
                    .groupSize(groupSize)
                    .index(chunkIndex++)
                    .bytes(chunkBytes)
                    .build());
        }
        assert groupSize == chunks.size();
        return chunks;
    }

    /**
     * Calculates the number of chunks that a byte array will be chopped into.
     *
     * @param bytes The byte array to chop into chunks.
     * @return The number of chunks.
     */
    private int numberOfChunks(byte @NonNull [] bytes) {
        int chunkCount = bytes.length / this.chunkCapacity;
        return bytes.length % this.chunkCapacity == 0 ? chunkCount : chunkCount + 1;
    }
}
