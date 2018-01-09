/*
 * Copyright 2018, The OpenNMS Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opennms.elasticsearch.search.aggregations.buckets.timeslice;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

public class TimeSliceAggregatorFactory extends MultiValuesSourceAggregatorFactory<ValuesSource.Numeric, TimeSliceAggregatorFactory> {
    private final DateHistogramInterval dateHistogramInterval;
    private final Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs;
    private final long interval;
    private final long offset;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final ExtendedBounds extendedBounds;
    private Rounding rounding;
    private final long start;
    private final long end;

    public TimeSliceAggregatorFactory(String name, Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs, long interval,
                                          DateHistogramInterval dateHistogramInterval, long offset, BucketOrder order, boolean keyed, long minDocCount,
                                          Rounding rounding, ExtendedBounds extendedBounds, SearchContext context, AggregatorFactory<?> parent,
                                          AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData, long start, long end) throws IOException {
        super(name, configs, context, parent, subFactoriesBuilder, metaData);
        this.configs = configs;
        this.interval = interval;
        this.dateHistogramInterval = dateHistogramInterval;
        this.offset = offset;
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.rounding = rounding;
        this.start = start;
        this.end = end;
    }

    public long minDocCount() {
        return minDocCount;
    }

    @Override
    public Aggregator createInternal(Aggregator parent, boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators,
                                     Map<String, Object> metaData) throws IOException {
        // Use a LinkedHashMap here to preserve ordering
        HashMap<String, ValuesSource.Numeric> valuesSources = new LinkedHashMap<>();

        for (Map.Entry<String, ValuesSourceConfig<ValuesSource.Numeric>> config : configs.entrySet()) {
            ValuesSource.Numeric vs = config.getValue().toValuesSource(context.getQueryShardContext());
            if (vs != null) {
                valuesSources.put(config.getKey(), vs);
            }
        }
        if (valuesSources.isEmpty()) {
            return createUnmapped(parent, pipelineAggregators, metaData);
        }
        return doCreateInternal(valuesSources, parent, collectsFromSingleBucket, pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(Map<String, ValuesSource.Numeric> valuesSources, Aggregator parent, boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, context, parent);
        }
        return createAggregator(valuesSources, parent, pipelineAggregators, metaData);
    }

    private Aggregator createAggregator(Map<String, ValuesSource.Numeric> valuesSources, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                                        Map<String, Object> metaData) throws IOException {
        return new TimeSliceAggregator(name, factories, rounding, offset, order, keyed, minDocCount, extendedBounds, valuesSources,
                configs, context, parent, pipelineAggregators, metaData, start, end);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        return createAggregator(null, parent, pipelineAggregators, metaData);
    }


}
