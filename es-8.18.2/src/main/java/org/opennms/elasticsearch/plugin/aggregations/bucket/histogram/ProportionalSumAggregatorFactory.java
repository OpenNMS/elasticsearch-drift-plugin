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

package org.opennms.elasticsearch.plugin.aggregations.bucket.histogram;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.histogram.LongBounds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

public class ProportionalSumAggregatorFactory extends MultiValuesSourceAggregatorFactory {

    private final Map<String, ValuesSourceConfig> configs;
    private final long offset;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final LongBounds extendedBounds;
    protected final DocValueFormat format;
    private Rounding rounding;
    private final Long start;
    private final Long end;
    private final String[] fieldNames;

    public ProportionalSumAggregatorFactory(String name, Map<String, ValuesSourceConfig> configs,
                                            long offset, BucketOrder order, boolean keyed, long minDocCount,
                                            Rounding rounding, Rounding shardRounding, LongBounds extendedBounds,
                                            DocValueFormat format, AggregationContext context, AggregatorFactory parent,
                                            AggregatorFactories.Builder subFactoriesBuilder,
                                            Map<String, Object> metaData, long start, long end, String[] fieldNames) throws IOException {
        super(name, configs, format, context, parent, subFactoriesBuilder, metaData);

        this.configs = configs;
        this.offset = offset;
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.format = format;
        this.rounding = rounding;
        this.start = start;
        this.end = end;
        this.fieldNames = fieldNames;
    }

    @Override
    protected Aggregator doCreateInternal(Map<String, ValuesSourceConfig> configs,
                                          DocValueFormat format, Aggregator parent, CardinalityUpperBound cardinality,
                                          Map<String, Object> metadata) throws IOException {
        return createAggregator(format, parent, cardinality, metadata);
    }

    private Aggregator createAggregator(DocValueFormat format, Aggregator parent,
                                        CardinalityUpperBound cardinality,
                                        Map<String, Object> metaData) throws IOException {

        // Compute offset so that the bucket start at the given start time
        long effectiveOffset = offset;
        if (start != null && effectiveOffset == 0) {
            final long delta = start - rounding.round(start);
            if (delta > 0) {
                effectiveOffset = delta;
            }
        }

        // HACK: Ensure we set some format by default since the caller expect a key_as_str
        // entry in the buckets
        DocValueFormat effectiveFormat = format;
        if (format == null || format == DocValueFormat.RAW) {
            effectiveFormat = DocValueFormat.RAW;
        }

        return new ProportionalSumAggregator(name, factories, rounding, effectiveOffset, order, keyed, minDocCount, extendedBounds, configs,
                effectiveFormat, context, parent, cardinality, metaData, start, end, fieldNames);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent,
                                        Map<String, Object> metadata)
            throws IOException {
        return createAggregator(null, parent, CardinalityUpperBound.NONE, metadata);
    }

}
