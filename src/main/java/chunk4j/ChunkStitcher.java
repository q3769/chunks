/*
 * MIT License
 *
 * Copyright (c) 2022 Qingtian Wang
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

import com.github.benmanes.caffeine.cache.*;
import lombok.Data;
import lombok.extern.java.Log;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.*;
import java.util.logging.Level;

/**
 * @author Qingtian Wang
 */
@Log
public final class ChunkStitcher implements Stitcher {

    private static final long DEFAULT_MAX_CHUNK_GROUP_COUNT = Long.MAX_VALUE;
    private static final long DEFAULT_MAX_STITCH_TIME_NANOS = Long.MAX_VALUE;

    private final Cache<UUID, Set<Chunk>> chunkGroups;
    private final Duration maxStitchTime;
    private final Long maxGroups;

    private ChunkStitcher(Builder builder) {
        maxStitchTime =
                builder.maxStitchTime == null ? Duration.ofNanos(DEFAULT_MAX_STITCH_TIME_NANOS) : builder.maxStitchTime;
        maxGroups = builder.maxGroups == null ? DEFAULT_MAX_CHUNK_GROUP_COUNT : builder.maxGroups;
        this.chunkGroups = Caffeine.newBuilder()
                .expireAfter(new SinceCreation(maxStitchTime))
                .maximumSize(maxGroups)
                .evictionListener(new InvoluntaryEvictionLogger())
                .build();
    }

    private static byte[] stitchAll(Set<Chunk> group) {
        log.log(Level.FINEST, () -> "Start stitching all chunks in " + group);
        assert ofSameId(group);
        byte[] groupBytes = new byte[getTotalByteSize(group)];
        List<Chunk> orderedGroup = new ArrayList<>(group);
        orderedGroup.sort(Comparator.comparingInt(Chunk::getIndex));
        int groupBytesPosition = 0;
        for (Chunk chunk : orderedGroup) {
            byte[] chunkBytes = chunk.getBytes();
            final int chunkBytesLength = chunkBytes.length;
            System.arraycopy(chunkBytes, 0, groupBytes, groupBytesPosition, chunkBytesLength);
            groupBytesPosition += chunkBytesLength;
        }
        log.log(Level.FINEST, () -> "End stitching all chunks in " + group);
        return groupBytes;
    }

    private static int getTotalByteSize(Set<Chunk> group) {
        return group.stream().mapToInt(chunk -> chunk.getBytes().length).sum();
    }

    private static boolean ofSameId(Set<Chunk> group) {
        return group.stream().map(Chunk::getGroupId).distinct().count() == 1;
    }

    @Override
    public Optional<byte[]> stitch(Chunk chunk) {
        log.log(Level.FINEST, () -> "received: " + chunk);
        final UUID groupId = chunk.getGroupId();
        CompleteGroupHolder completeGroupHolder = new CompleteGroupHolder();
        chunkGroups.asMap().compute(groupId, (gid, group) -> {
            if (group == null) {
                group = new HashSet<>();
            }
            if (!group.add(chunk)) {
                log.log(Level.WARNING, "received duplicate chunk: {0}", chunk);
            }
            int received = group.size();
            int expected = chunk.getGroupSize();
            if (received != expected) {
                log.log(Level.FINER,
                        () -> "received [" + received + "] chunks while expecting [" + expected
                                + "] before starting to stitch and restore, keeping group [" + groupId + "] in cache");
                return group;
            }
            log.log(Level.FINER,
                    () -> "received all [" + expected
                            + "] expected chunks, starting to stitch and restore original data, evicting group ["
                            + groupId + "] from cache");
            completeGroupHolder.setCompleteGroupOfChunks(group);
            return null;
        });
        Set<Chunk> groupChunks = completeGroupHolder.getCompleteGroupOfChunks();
        if (groupChunks == null) {
            return Optional.empty();
        }
        return Optional.of(stitchAll(groupChunks));
    }

    private static class SinceCreation implements Expiry<UUID, Set<Chunk>> {

        private final Duration duration;

        SinceCreation(Duration duration) {
            this.duration = duration;
        }

        @Override
        public long expireAfterCreate(UUID uuid, Set<Chunk> chunks, long currentTime) {
            return duration.toNanos();
        }

        @Override
        public long expireAfterUpdate(UUID uuid, Set<Chunk> chunks, long currentTime, long currentDuration) {
            return currentDuration;
        }

        @Override
        public long expireAfterRead(UUID uuid, Set<Chunk> chunks, long currentTime, long currentDuration) {
            return currentDuration;
        }
    }

    public static class Builder {

        private Duration maxStitchTime;
        private Long maxGroups;

        public Builder maxStitchTime(Duration maxStitchTime) {
            this.maxStitchTime = maxStitchTime;
            return this;
        }

        public Builder maxGroups(long maxGroups) {
            this.maxGroups = maxGroups;
            return this;
        }

        public ChunkStitcher build() {
            return new ChunkStitcher(this);
        }
    }

    @Data
    private static class CompleteGroupHolder {

        Set<Chunk> completeGroupOfChunks;
    }

    private class InvoluntaryEvictionLogger implements RemovalListener<UUID, Set<Chunk>> {

        @Override
        public void onRemoval(UUID groupId, Set<Chunk> chunks, @Nonnull RemovalCause cause) {
            switch (cause) {
                case EXPIRED:
                    log.log(Level.SEVERE,
                            "chunk group [{0}] took too long to stitch and expired after [{1}], expecting [{2}] chunks but only received [{3}] when expired",
                            new Object[] { groupId, maxStitchTime,
                                    chunks.stream().findFirst().orElseThrow(NoSuchElementException::new).getGroupSize(),
                                    chunks.size() });
                    break;
                case SIZE:
                    log.log(Level.SEVERE,
                            "chunk group [{0}] was removed due to exceeding max group count [{1}]",
                            new Object[] { groupId, maxGroups });
                    break;
                case EXPLICIT:
                case REPLACED:
                case COLLECTED:
                    break;
                default:
                    throw new AssertionError("Unexpected eviction cause: " + cause.name());
            }
        }
    }
}
