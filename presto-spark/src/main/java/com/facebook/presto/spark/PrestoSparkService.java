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
package com.facebook.presto.spark;

import com.facebook.presto.spark.execution.PrestoTaskCompiler;
import com.facebook.presto.spark.spi.QueryExecutionFactory;
import com.facebook.presto.spark.spi.Service;
import com.facebook.presto.spark.spi.TaskCompiler;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PrestoSparkService
        implements Service
{
    private final PrestoSparkQueryExecutionFactory queryExecutionFactory;
    private final PrestoTaskCompiler taskCompiler;

    @Inject
    public PrestoSparkService(PrestoSparkQueryExecutionFactory queryExecutionFactory, PrestoTaskCompiler taskCompiler)
    {
        this.queryExecutionFactory = requireNonNull(queryExecutionFactory, "queryExecutionFactory is null");
        this.taskCompiler = requireNonNull(taskCompiler, "taskCompiler is null");
    }

    @Override
    public QueryExecutionFactory createQueryExecutionFactory()
    {
        return queryExecutionFactory;
    }

    @Override
    public TaskCompiler createTaskCompiler()
    {
        return taskCompiler;
    }
}