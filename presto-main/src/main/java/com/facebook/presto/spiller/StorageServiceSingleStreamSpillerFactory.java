package com.facebook.presto.spiller;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.SpillContext;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.spiller.SpillCipher;
import com.facebook.presto.spi.storage.StorageService;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class StorageServiceSingleStreamSpillerFactory
        implements SingleStreamSpillerFactory
{
    private final ListeningExecutorService executor;
    private final PagesSerdeFactory serdeFactory;
    private final SpillerStats spillerStats;
    private final boolean spillEncryptionEnabled;
    private final StorageService storageService;

    public StorageServiceSingleStreamSpillerFactory(
            ListeningExecutorService executor,
            PagesSerdeFactory serdeFactory,
            SpillerStats spillerStats,
            boolean spillEncryptionEnabled,
            StorageService storageService)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats is null");
        this.spillEncryptionEnabled = spillEncryptionEnabled;
        this.storageService = requireNonNull(storageService, "storageService is null");
    }

    @Override
    public SingleStreamSpiller create(List<Type> types, SpillContext spillContext, LocalMemoryContext memoryContext)
    {
        Optional<SpillCipher> spillCipher = Optional.empty();
        if (spillEncryptionEnabled) {
            spillCipher = Optional.of(new AesSpillCipher());
        }
        PagesSerde serde = serdeFactory.createPagesSerdeForSpill(spillCipher);
        return new StorageServiceSingleStreamSpiller(
                storageService,
                executor,
                serde,
                // TODO: configure it!
                24 * 1024 * 1024,
                spillerStats,
                spillContext,
                memoryContext,
                spillCipher);
    }
}
