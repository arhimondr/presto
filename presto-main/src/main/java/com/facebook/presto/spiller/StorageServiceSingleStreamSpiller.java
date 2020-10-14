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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.common.io.PageDataOutput;
import com.facebook.presto.common.io.SerializedPage;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.SpillContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.PagesSerdeUtil;
import com.facebook.presto.spi.spiller.SpillCipher;
import com.facebook.presto.spi.storage.StorageHandle;
import com.facebook.presto.spi.storage.StorageService;
import com.facebook.presto.spi.storage.StorageSink;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.slice.InputStreamSliceInput;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class StorageServiceSingleStreamSpiller
        implements SingleStreamSpiller
{
    private final StorageService storageService;
    private final ListeningExecutorService executor;
    private final PagesSerde serde;
    private final int bufferSizeInBytes;
    private final SpillerStats spillerStats;
    private final SpillContext localSpillContext;
    private final LocalMemoryContext memoryContext;
    private final Optional<SpillCipher> spillCipher;

    private final Closer closer = Closer.create();

    private StorageSink sink;
    private int bufferedBytes;
    private List<DataOutput> bufferedPages = new ArrayList<>();
    private boolean sealed;
    private StorageHandle storageHandle;
    private long spilledPagesInMemorySizeInBytes;
    private ListenableFuture<?> spillInProgress = Futures.immediateFuture(null);

    public StorageServiceSingleStreamSpiller(
            StorageService storageService,
            ListeningExecutorService executor,
            PagesSerde serde,
            int bufferSizeInBytes,
            SpillerStats spillerStats,
            SpillContext localSpillContext,
            LocalMemoryContext memoryContext,
            Optional<SpillCipher> spillCipher)
    {
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.serde = requireNonNull(serde, "serde is null");
        this.bufferSizeInBytes = requireNonNull(bufferSizeInBytes, "bufferSizeInBytes is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats is null");
        this.localSpillContext = requireNonNull(localSpillContext, "localSpillContext is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.spillCipher = requireNonNull(spillCipher, "spillCipher is null");
        checkState(!spillCipher.isPresent() || !spillCipher.get().isDestroyed(), "spillCipher is already destroyed");
        this.spillCipher.ifPresent(cipher -> closer.register(cipher::destroy));
    }

    @Override
    public ListenableFuture<?> spill(Iterator<Page> pages)
    {
        checkState(!sealed, "spiller is sealed");
        checkNoSpillInProgress();
        spillInProgress = executor.submit(() -> storePages(pages));
        return spillInProgress;
    }

    private void storePages(Iterator<Page> pages)
    {
        checkState(!sealed, "spiller is sealed");

        if (sink == null) {
            sink = storageService.create();
            memoryContext.setBytes(sink.getRetainedSizeInBytes());
        }

        while (pages.hasNext()) {
            Page page = pages.next();
            spilledPagesInMemorySizeInBytes += page.getSizeInBytes();
            SerializedPage serializedPage = serde.serialize(page);
            PageDataOutput pageDataOutput = new PageDataOutput(serializedPage);
            localSpillContext.updateBytes(pageDataOutput.size());
            spillerStats.addToTotalSpilledBytes(pageDataOutput.size());
            if (bufferedBytes + pageDataOutput.size() < bufferSizeInBytes) {
                bufferedPages.add(pageDataOutput);
                bufferedBytes += pageDataOutput.size();
            }
            else {
                try {
                    sink.write(bufferedPages);
                }
                catch (IOException e) {
                    // TODO: external error
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to write spilled pages", e);
                }
                memoryContext.setBytes(sink.getRetainedSizeInBytes());
                bufferedPages.clear();
                bufferedBytes = 0;
            }
        }
    }

    @Override
    public Iterator<Page> getSpilledPages()
    {
        checkNoSpillInProgress();
        return readPages();
    }

    @Override
    public long getSpilledPagesInMemorySize()
    {
        return spilledPagesInMemorySizeInBytes;
    }

    @Override
    public ListenableFuture<List<Page>> getAllSpilledPages()
    {
        checkNoSpillInProgress();
        return executor.submit(() -> ImmutableList.copyOf(getSpilledPages()));
    }

    private Iterator<Page> readPages()
    {
        sealed = true;

        if (storageHandle == null) {
            if (sink == null) {
                return emptyIterator();
            }
            if (!bufferedPages.isEmpty()) {
                try {
                    sink.write(bufferedPages);
                }
                catch (IOException e) {
                    // TODO: external error
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to write spilled pages", e);
                }
                memoryContext.setBytes(sink.getRetainedSizeInBytes());
                bufferedPages.clear();
                bufferedBytes = 0;
            }
            try {
                storageHandle = sink.commit();
                sink = null;
                memoryContext.setBytes(0);
            }
            catch (IOException e) {
                // TODO: external error
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to write spilled pages", e);
            }
        }

        InputStream stream = closer.register(storageService.open(storageHandle));
        Iterator<Page> pages = PagesSerdeUtil.readPages(serde, new InputStreamSliceInput(stream));
        return closeWhenExhausted(pages, stream);
    }

    @Override
    public void close()
    {
        if (sink != null && storageHandle == null) {
            closer.register(() -> sink.rollback());
        }
        if (storageHandle != null) {
            closer.register(() -> storageService.remove(storageHandle));
        }
        closer.register(localSpillContext);
        closer.register(() -> memoryContext.setBytes(0));
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to close spiller", e);
        }
    }

    private void checkNoSpillInProgress()
    {
        checkState(spillInProgress.isDone(), "spill in progress");
    }

    // TODO: based on the FileSingleStreamSpiller#closeWhenExhausted
    // TODO: move to some utils
    private static <T> Iterator<T> closeWhenExhausted(Iterator<T> iterator, Closeable resource)
    {
        requireNonNull(iterator, "iterator is null");
        requireNonNull(resource, "resource is null");

        return new AbstractIterator<T>()
        {
            @Override
            protected T computeNext()
            {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                try {
                    resource.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return endOfData();
            }
        };
    }
}
