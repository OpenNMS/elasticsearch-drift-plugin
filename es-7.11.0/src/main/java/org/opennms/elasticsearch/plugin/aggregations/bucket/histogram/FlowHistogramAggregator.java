/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2021 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2021 The OpenNMS Group, Inc.
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
import java.util.Map;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.histogram.DoubleBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.RangeHistogramAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

/**
 * A RangeHistogramAggregator that gives access to its interval and offset and allows to convert bucket ordinals
 * into their bucket key.
 *
 * Note: Hopefully, ES can be convinced to add the necessary methods to RangeHistogramAggregator. This would make
 * this class and its companion FlowHistogramAggregationBuilder unnecessary.
 */
public class FlowHistogramAggregator extends RangeHistogramAggregator {

    public FlowHistogramAggregator(
            String name,
            AggregatorFactories factories,
            double interval,
            double offset,
            BucketOrder order,
            boolean keyed,
            long minDocCount,
            DoubleBounds extendedBounds,
            DoubleBounds hardBounds,
            ValuesSourceConfig valuesSourceConfig,
            AggregationContext context,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
    ) throws IOException {
        super(
                name,
                factories,
                interval,
                offset,
                order,
                keyed,
                minDocCount,
                extendedBounds,
                hardBounds,
                valuesSourceConfig,
                context,
                parent,
                cardinality,
                metadata
        );
    }

    public double getInterval() {
        return interval;
    }

    public double getOffset() {
        return offset;
    }

    /**
     * Converts bucket ordinals into the keys of the buckets.
     *
     * Range histograms use interval numbers as bucket keys. For a range the first and last bucket keys are
     * calculated by:
     *
     * <pre>
     * {@code
     *    final double startKey = Math.floor((effectiveFrom - offset) / interval);
     *    final double endKey = Math.floor((effectiveTo - offset) / interval);
     * }
     * </pre>
     *
     * Note: Later on, when buckets are output bucket keys are transformed into their interval starts
     * (cf. {@link org.elasticsearch.search.aggregations.bucket.histogram.AbstractHistogramAggregator#buildAggregations(long[])}).
     * The calculation is:
     *
     * <pre>
     * {@code
     *   double key = bucketKey * interval + offset;
     * }
     * </pre>
     */
    public double getBucketKey(long bucket) {
        return Double.longBitsToDouble(bucketOrds.get(bucket));
    }

}
