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
package com.facebook.presto.spark.util;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spark.classloader_interface.PrestoSparkRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.AbstractIterator;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Arrays.copyOfRange;

public class PrestoSparkUtils
{
    private PrestoSparkUtils() {}

    public static Iterator<Page> transformRowsToPages(Iterator<PrestoSparkRow> rows, List<Type> types)
    {
        return new AbstractIterator<Page>()
        {
            @Override
            protected Page computeNext()
            {
                if (!rows.hasNext()) {
                    return endOfData();
                }
                PageBuilder pageBuilder = new PageBuilder(types);
                while (rows.hasNext() && !pageBuilder.isFull()) {
                    PrestoSparkRow row = rows.next();
                    SliceInput sliceInput = new BasicSliceInput(wrappedBuffer(row.getBytes(), 0, row.getLength()));
                    pageBuilder.declarePosition();
                    for (int channel = 0; channel < types.size(); channel++) {
                        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                        blockBuilder.readPositionFrom(sliceInput);
                    }
                    sliceInput.close();
                }
                verify(!pageBuilder.isEmpty());
                return pageBuilder.build();
            }
        };
    }

    public static PrestoSparkSerializedPage toPrestoSparkSerializedPage(SerializedPage serializedPage)
    {
        Slice slice = serializedPage.getSlice();
        checkArgument(slice.hasByteArray(), "slice is expected to be based on a byte array");
        byte[] array = slice.byteArray();
        if (slice.byteArrayOffset() != 0 || slice.length() != array.length) {
            array = copyOfRange(array, slice.byteArrayOffset(), slice.byteArrayOffset() + slice.length());
        }
        return new PrestoSparkSerializedPage(
                array,
                serializedPage.getPositionCount(),
                serializedPage.getUncompressedSizeInBytes(),
                serializedPage.getPageCodecMarkers());
    }

    public static SerializedPage toSerializedPage(PrestoSparkSerializedPage prestoSparkSerializedPage)
    {
        return new SerializedPage(
                Slices.wrappedBuffer(prestoSparkSerializedPage.getBytes()),
                prestoSparkSerializedPage.getPageCodecMarkers(),
                prestoSparkSerializedPage.getPositionCount(),
                prestoSparkSerializedPage.getUncompressedSizeInBytes());
    }

    public static <T> Iterator<T> getNullifyingIterator(List<T> list)
    {
        return new AbstractIterator<T>()
        {
            private int index;

            @Override
            protected T computeNext()
            {
                if (index >= list.size()) {
                    return endOfData();
                }
                T element = list.get(index);
                list.set(index, null);
                index++;
                return element;
            }
        };
    }
}