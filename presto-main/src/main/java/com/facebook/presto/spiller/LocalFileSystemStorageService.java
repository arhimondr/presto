/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spiller;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.storage.StorageHandle;
import com.facebook.presto.spi.storage.StorageService;
import com.facebook.presto.spi.storage.StorageSink;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.OUT_OF_SPILL_SPACE;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.getFileStore;

public class LocalFileSystemStorageService
        implements StorageService
{
    private static final Logger log = Logger.get(FileSingleStreamSpillerFactory.class);

    private final List<Path> spillPaths;
    private final double maxUsedSpaceThreshold;
    private int roundRobinIndex;

    public LocalFileSystemStorageService(List<Path> storagePaths, double maxUsedSpaceThreshold)
    {
        this.spillPaths = storagePaths;
        spillPaths.forEach(LocalFileSystemStorageService::cleanupStorage);
        storagePaths.forEach(path -> {
            try {
                createDirectories(path);
            }
            catch (IOException e) {
                throw new IllegalArgumentException(
                        format("could not create spill path %s; adjust experimental.spiller-spill-path config property or filesystem permissions", path), e);
            }
            if (!path.toFile().canWrite()) {
                throw new IllegalArgumentException(
                        format("spill path %s is not writable; adjust experimental.spiller-spill-path config property or filesystem permissions", path));
            }
        });
        this.maxUsedSpaceThreshold = maxUsedSpaceThreshold;
    }

    private static void cleanupStorage(Path path)
    {
        // this method removes everything in the local storage directory
        // the assumption is that the data stored locally shouldn't survive restarts
        try {
            Files.deleteIfExists(path);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public StorageSink create()
            throws IOException
    {
        // TODO
        return null;
    }

    @Override
    public InputStream open(StorageHandle handle)
    {
        // TODO
        return null;
    }

    @Override
    public void remove(StorageHandle handle)
    {
        // TODO
    }

    private synchronized Path getNextSpillPath()
    {
        int spillPathsCount = spillPaths.size();
        for (int i = 0; i < spillPathsCount; ++i) {
            int pathIndex = (roundRobinIndex + i) % spillPathsCount;
            Path path = spillPaths.get(pathIndex);
            if (hasEnoughDiskSpace(path)) {
                roundRobinIndex = (roundRobinIndex + i + 1) % spillPathsCount;
                return path;
            }
        }
        if (spillPaths.isEmpty()) {
            throw new PrestoException(OUT_OF_SPILL_SPACE, "No spill paths configured");
        }
        throw new PrestoException(OUT_OF_SPILL_SPACE, "No free space available for spill");
    }

    private boolean hasEnoughDiskSpace(Path path)
    {
        try {
            FileStore fileStore = getFileStore(path);
            return fileStore.getUsableSpace() > fileStore.getTotalSpace() * (1.0 - maxUsedSpaceThreshold);
        }
        catch (IOException e) {
            throw new PrestoException(OUT_OF_SPILL_SPACE, "Cannot determine free space for spill", e);
        }
    }
}
