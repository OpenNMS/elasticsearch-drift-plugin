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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

/**
 * This bucket aggregator determines allows documents to be added into many
 * buckets based on their date range and keeps track of the sum of a given value
 * relative the how long the documents spend in that bucket.
 *
 * @author jwhite
 */
public class ProportionalSumAggregator extends BucketsAggregator {

    /** Multiple ValuesSource with field names */
    private final MultiValuesSource.NumericMultiValuesSource valuesSources;
    private final DocValueFormat formatter;
    private final Rounding rounding;
    private final BucketOrder order;
    private final boolean keyed;

    private final long minDocCount;
    private final ExtendedBounds extendedBounds;

    private final LongHash bucketOrds;
    private final long offset;
    private DoubleArray sums;

    private final Long start;
    private final Long end;

    ProportionalSumAggregator(String name, AggregatorFactories factories, Rounding rounding, long offset, BucketOrder order,
                        boolean keyed,
                        long minDocCount, @Nullable ExtendedBounds extendedBounds, @Nullable Map<String, ValuesSource.Numeric> valuesSources,
                        DocValueFormat formatter, SearchContext aggregationContext, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                        Map<String, Object> metaData, Long start, Long end) throws IOException {

        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.rounding = rounding;
        this.offset = offset;
        this.order = InternalOrder.validate(order, this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.formatter = formatter;
        this.start = start != null ? start : Long.MIN_VALUE;
        this.end = end != null ? end : Long.MAX_VALUE;

        if (valuesSources != null && !valuesSources.isEmpty()) {
            this.valuesSources = new MultiValuesSource.NumericMultiValuesSource(valuesSources, MultiValueMode.MIN);
        } else {
            this.valuesSources = null;
        }

        bucketOrds = new LongHash(1, aggregationContext.bigArrays());
        sums = context.bigArrays().newDoubleArray(1, true);
    }

    @Override
    public boolean needsScores() {
        return ((valuesSources != null) && valuesSources.needsScores()) || super.needsScores();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final NumericDoubleValues[] values = new NumericDoubleValues[valuesSources.fieldNames().length];
        for (int i = 0; i < values.length; ++i) {
            values[i] = valuesSources.getField(i, ctx);
        }

        return new LeafBucketCollectorBase(sub, values) {
            final long[] fieldVals = new long[valuesSources.fieldNames().length];

            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (fieldVals.length != 4) {
                    throw new IllegalStateException("Invalid number of fields specified. Need 4, got " + fieldVals.length);
                }

                // start of the range
                long rangeStartVal = 0;
                final NumericDoubleValues rangeStartDoubleValues = values[0];
                if (rangeStartDoubleValues.advanceExact(doc)) {
                    rangeStartVal = ((Double)rangeStartDoubleValues.doubleValue()).longValue();
                }
                if (rangeStartVal < 0) {
                    throw new IllegalArgumentException("Invalid range start: " + rangeStartVal);
                }

                // end of the range
                long rangeEndVal = 0;
                final NumericDoubleValues rangeEndDoubleValues = values[1];
                if (rangeEndDoubleValues.advanceExact(doc)) {
                    rangeEndVal = ((Double)rangeEndDoubleValues.doubleValue()).longValue();
                }
                if (rangeEndVal < 0) {
                    throw new IllegalArgumentException("Invalid range end: " + rangeEndVal);
                }
                if (rangeEndVal < rangeStartVal) {
                    throw new IllegalArgumentException("Start cannot be after end! start: " +
                            rangeStartVal + " end: " + rangeEndVal);
                }

                // duration of the range
                final Long rangeDuration = rangeEndVal - rangeStartVal;

                // the actual value
                double valueVal = Double.NaN;
                final NumericDoubleValues valueDoubleValues = values[2];
                if (valueDoubleValues.advanceExact(doc)) {
                    valueVal = valueDoubleValues.doubleValue();
                }

                // scale value by sampling interval
                final NumericDoubleValues samplingDoubleValues = values[3];
                if (samplingDoubleValues.advanceExact(doc)) {
                    valueVal *= samplingDoubleValues.doubleValue();
                }

                // round the first value
                long startRounded = rounding.round(Math.max(rangeStartVal, start) - offset) + offset;

                // round the last value
                long lastRounded = rounding.round(Math.min(rangeEndVal, end) - offset) + offset;

                // add to all the buckets between first and last
                long bucketStart = startRounded;
                while (bucketStart <= lastRounded) {
                    long nextBucketStart = rounding.nextRoundingValue(bucketStart);

                    // calculate the ratio of time spent in this bucket
                    double bucketRatio;
                    if (rangeDuration != 0) {
                        long timeInBucket = getTimeInWindow(bucketStart, nextBucketStart, rangeStartVal, rangeEndVal);
                        bucketRatio = timeInBucket / rangeDuration.doubleValue();
                    } else {
                        // start=end, so the document can only be in a single bucket, use the complete value
                        bucketRatio = 1d;
                    }

                    // calculate the value that is proportional to the time spent in this bucket
                    double proportionalValue = valueVal * bucketRatio;

                    long bucketOrd = bucketOrds.add(bucketStart);
                    if (bucketOrd < 0) { // already seen
                        bucketOrd = -1 - bucketOrd;
                        collectExistingBucket(sub, doc, bucketOrd);
                    } else {
                        collectBucket(sub, doc, bucketOrd);
                        sums = bigArrays.grow(sums, bucketOrd + 1);
                    }
                    sums.increment(bucketOrd, proportionalValue);

                    bucketStart = nextBucketStart;
                }
            }
        };
    }

    public static long getTimeInWindow(long windowStart, long windowEnd, long rangeStart, long rangeEnd) {
        if (rangeStart > windowEnd || rangeEnd < windowStart) {
            // No overlap
            return 0L;
        }
        return Math.min(windowEnd, rangeEnd) - Math.max(windowStart, rangeStart);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        List<InternalProportionalSumHistogram.Bucket> buckets = new ArrayList<>((int) bucketOrds.size());
        for (long i = 0; i < bucketOrds.size(); i++) {
            final long bucketOrd = bucketOrds.get(i);
            buckets.add(new InternalProportionalSumHistogram.Bucket(bucketOrd, bucketDocCount(i), sums.get(i), keyed, formatter, bucketAggregations(i)));
        }

        // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
        CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator(this));

        // value source will be null for unmapped fields
        InternalProportionalSumHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalProportionalSumHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                : null;
        return new InternalProportionalSumHistogram(name, buckets, order, minDocCount, offset, emptyBucketInfo, formatter, keyed,
                pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalProportionalSumHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalProportionalSumHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                : null;
        return new InternalProportionalSumHistogram(name, Collections.emptyList(), order, minDocCount, offset, emptyBucketInfo, formatter, keyed,
                pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
        Releasables.close(sums);
    }
}
