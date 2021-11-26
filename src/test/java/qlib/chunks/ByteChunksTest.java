/*
 * The MIT License
 * Copyright 2021 Qingtian Wang.
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package qlib.chunks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Qingtian Wang
 */
public class ByteChunksTest {

    private static final String DATA_TEXT1 =
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    private static final String DATA_TEXT2 = DATA_TEXT1 + DATA_TEXT1;

    @Test
    public void testByteChunks() {
        final int chunkByteCapacity = 4;
        final DefaultChopper chopper = DefaultChopper.ofChunkByteCapacity(chunkByteCapacity);
        final DefaultStitcher stitcher = new DefaultStitcher.Builder().build();

        final List<Chunk> choppedAndShuffled = new ArrayList<>();
        choppedAndShuffled.addAll(chopper.chop(DATA_TEXT1.getBytes()));
        choppedAndShuffled.addAll(chopper.chop(DATA_TEXT2.getBytes()));
        Collections.shuffle(choppedAndShuffled);
        final List<byte[]> stitched = new ArrayList<>();
        for (Chunk chunk : choppedAndShuffled) {
            final Optional<byte[]> stitchBytes = stitcher.stitch(chunk);
            if (stitchBytes.isEmpty())
                continue;
            stitched.add(stitchBytes.get());
        }

        final int originalDataCount = 2;
        assertEquals(originalDataCount, stitched.size());
        final String dataStitched1 = new String(stitched.get(0));
        final String dataStitched2 = new String(stitched.get(1));
        if (!DATA_TEXT1.equals(dataStitched1))
            assertEquals(DATA_TEXT2, dataStitched1);
        else
            assertEquals(DATA_TEXT2, dataStitched2);

    }
}