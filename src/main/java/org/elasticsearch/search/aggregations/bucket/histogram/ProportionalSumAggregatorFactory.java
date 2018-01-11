/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2018 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2018 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.elasticsearch.search.aggregations.bucket.histogram;

import static org.elasticsearch.search.aggregations.bucket.histogram.ProportionalSumAggregationBuilder.getValueSourceForRangeStart;

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
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

public class ProportionalSumAggregatorFactory extends MultiValuesSourceAggregatorFactory<ValuesSource.Numeric, ProportionalSumAggregatorFactory> {

    private final Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs;
    private final long offset;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final ExtendedBounds extendedBounds;
    private Rounding rounding;
    private final Long start;
    private final Long end;

    public ProportionalSumAggregatorFactory(String name, Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs,
                                            long offset, BucketOrder order, boolean keyed, long minDocCount,
                                            Rounding rounding, ExtendedBounds extendedBounds, SearchContext context,
                                            AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder,
                                            Map<String, Object> metaData, long start, long end) throws IOException {
        super(name, configs, context, parent, subFactoriesBuilder, metaData);
        this.configs = configs;
        this.offset = offset;
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.rounding = rounding;
        this.start = start;
        this.end = end;
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
        if (!collectsFromSingleBucket) {
            return asMultiBucketAggregator(this, context, parent);
        }
        return createAggregator(valuesSources, parent, pipelineAggregators, metaData);
    }

    private Aggregator createAggregator(Map<String, ValuesSource.Numeric> valuesSources, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                                        Map<String, Object> metaData) throws IOException {
        return new ProportionalSumAggregator(name, factories, rounding, offset, order, keyed, minDocCount, extendedBounds, valuesSources,
                getValueSourceForRangeStart(configs).format(), context, parent, pipelineAggregators, metaData, start, end);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        return createAggregator(null, parent, pipelineAggregators, metaData);
    }
}
