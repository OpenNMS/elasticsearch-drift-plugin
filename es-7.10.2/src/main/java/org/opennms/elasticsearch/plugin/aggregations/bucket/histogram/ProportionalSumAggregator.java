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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.LongBounds;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
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
    private final LongBounds extendedBounds;

    private final LongKeyedBucketOrds bucketOrds;
    private final long offset;
    private DoubleArray sums;

    private final Long start;
    private final Long end;

    private final String[] fieldNames;

    ProportionalSumAggregator(String name, AggregatorFactories factories, Rounding rounding, long offset, BucketOrder order,
                              boolean keyed,
                              long minDocCount, LongBounds extendedBounds, Map<String, ValuesSourceConfig> valuesSourceConfigs,
                              DocValueFormat formatter, SearchContext aggregationContext, Aggregator parent, CardinalityUpperBound bucketCardinality,
                              Map<String, Object> metaData, Long start, Long end, String[] fieldNames) throws IOException {

        super(name, factories, aggregationContext, parent, bucketCardinality, metaData);
        this.rounding = rounding;
        this.offset = offset;
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.formatter = formatter;
        this.start = start != null ? start : Long.MIN_VALUE;
        this.end = end != null ? end : Long.MAX_VALUE;
        this.fieldNames = fieldNames;

        if (valuesSourceConfigs != null && !valuesSourceConfigs.isEmpty()) {
            this.valuesSources = new MultiValuesSource.NumericMultiValuesSource(valuesSourceConfigs, context.getQueryShardContext());
        } else {
            this.valuesSources = null;
        }

        bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), bucketCardinality);
        sums = context.bigArrays().newDoubleArray(1, true);

        order.validate(this);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final OrderedValueReferences orderedValueReferences = new OrderedValueReferences(ctx, valuesSources, fieldNames);
        final SortedNumericDoubleValues[] values = orderedValueReferences.getValuesArray();

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                // start of the range
                final SortedNumericDoubleValues rangeStartDoubleValues = orderedValueReferences.getRangeStarts();
                long rangeStartVal = 0;
                if (rangeStartDoubleValues.advanceExact(doc)) {
                    rangeStartVal = ((Double)rangeStartDoubleValues.nextValue()).longValue();
                }
                if (rangeStartVal < 0) {
                    throw new IllegalArgumentException("Invalid range start: " + rangeStartVal);
                }

                // end of the range
                final SortedNumericDoubleValues rangeEndDoubleValues = orderedValueReferences.getRangeEnds();
                long rangeEndVal = 0;
                if (rangeEndDoubleValues.advanceExact(doc)) {
                    rangeEndVal = ((Double)rangeEndDoubleValues.nextValue()).longValue();
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
                final SortedNumericDoubleValues valueDoubleValues = orderedValueReferences.getValues();
                double valueVal = Double.NaN;
                if (valueDoubleValues.advanceExact(doc)) {
                    valueVal = valueDoubleValues.nextValue();
                }

                // scale value by sampling interval
                final SortedNumericDoubleValues samplingDoubleValues = orderedValueReferences.getSamplings().orElse(null);
                if (samplingDoubleValues != null) {
                    if (samplingDoubleValues.advanceExact(doc)) {
                        final Double samplingValue = samplingDoubleValues.nextValue();
                        if (Double.isFinite(samplingValue) && samplingValue != 0.0) {
                            valueVal *= samplingValue;
                        }
                    }
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

                    long bucketOrd = bucketOrds.add(owningBucketOrd, bucketStart);
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

    /**
     * Based on BucketsAggregator.buildAggregationsForVariable (ES 7.10.2)
     *
     * Uses an extended BucketBuilderForVariableWithOwningBucket interface that provides the
     * {@code owningBucketOrd} to bucket builder calls.
     *
     * The {@code owningBucketOrd} is required by the builder in order to determine the index
     * of the corresponding sum.
     */
    protected final <B> InternalAggregation[] buildAggregationsForVariableBuckets(
            long[] owningBucketOrds,
            BucketBuilderForVariableWithOwningBucket<B> bucketBuilder,
            ResultBuilderForVariable<B> resultBuilder
    ) throws IOException {
        long totalOrdsToCollect = 0;
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            totalOrdsToCollect += bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);
        }
        if (totalOrdsToCollect > Integer.MAX_VALUE) {
            throw new AggregationExecutionException("Can't collect more than [" + Integer.MAX_VALUE
                                                    + "] buckets but attempted [" + totalOrdsToCollect + "]");
        }
        long[] bucketOrdsToCollect = new long[(int) totalOrdsToCollect];
        int b = 0;
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            while(ordsEnum.next()) {
                bucketOrdsToCollect[b++] = ordsEnum.ord();
            }
        }
        InternalAggregations[] subAggregationResults = buildSubAggsForBuckets(bucketOrdsToCollect);

        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        b = 0;
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            List<B> buckets = new ArrayList<>((int) bucketOrds.size());
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            while(ordsEnum.next()) {
                if (bucketOrdsToCollect[b] != ordsEnum.ord()) {
                    throw new AggregationExecutionException("Iteration order of [" + bucketOrds + "] changed without mutating. ["
                                                            + ordsEnum.ord() + "] should have been [" + bucketOrdsToCollect[b] + "]");
                }
                buckets.add(bucketBuilder.build(owningBucketOrds[ordIdx], ordsEnum.value(), bucketDocCount(ordsEnum.ord()), subAggregationResults[b++]));
            }
            results[ordIdx] = resultBuilder.build(owningBucketOrds[ordIdx], buckets);
        }
        return results;
    }
    @FunctionalInterface
    protected interface BucketBuilderForVariableWithOwningBucket<B> {
        B build(long owningBucketOrd, long bucketValue, int docCount, InternalAggregations subAggregationResults);
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForVariableBuckets(owningBucketOrds,
                (owningBucketOrd, bucketValue, docCount, subAggregationResults) -> {
                    long idx = bucketOrds.find(owningBucketOrd, bucketValue);
                    return new InternalProportionalSumHistogram.Bucket(bucketValue, docCount, sums.get(idx), keyed, formatter, subAggregationResults);
                }, (owningBucketOrd, buckets) -> {
                    // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
                    CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

                    // value source will be null for unmapped fields
                    // Important: use `rounding` here, not `shardRounding`
                    InternalProportionalSumHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                                                                            ? new InternalProportionalSumHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                                                                            : null;
                    return new InternalProportionalSumHistogram(name, buckets, order, minDocCount, offset, emptyBucketInfo, formatter,
                            keyed, metadata());
                });
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalProportionalSumHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalProportionalSumHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                : null;
        return new InternalProportionalSumHistogram(name, Collections.emptyList(), order, minDocCount, offset, emptyBucketInfo, formatter, keyed,
                metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
        Releasables.close(sums);
    }
}
