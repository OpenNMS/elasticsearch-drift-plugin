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
import java.util.ArrayList;
import java.util.Arrays;
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
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

public class TimeSliceAggregator extends BucketsAggregator {

    /** Multiple ValuesSource with field names */
    private final MultiValuesSource.NumericMultiValuesSource valuesSources;
    private final Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs;
    private final DocValueFormat formatter;
    private final Rounding rounding;
    private final BucketOrder order;
    private final boolean keyed;

    private final long minDocCount;
    private final ExtendedBounds extendedBounds;

    private final LongHash bucketOrds;
    private final long step;
    private final long offset;
    private DoubleArray sums;

    private final long start;
    private final long end;


    TimeSliceAggregator(String name, AggregatorFactories factories, Rounding rounding, long offset, BucketOrder order,
                        boolean keyed,
                        long minDocCount, @Nullable ExtendedBounds extendedBounds, @Nullable Map<String, ValuesSource.Numeric> valuesSources,
                        Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs, SearchContext aggregationContext,
                        Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
                        long start, long end) throws IOException {

        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.rounding = rounding;
        this.offset = offset;
        this.order = InternalOrder.validate(order, this);;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.configs = configs;

        if (valuesSources != null && !valuesSources.isEmpty()) {
            this.valuesSources = new MultiValuesSource.NumericMultiValuesSource(valuesSources, MultiValueMode.MIN);
        } else {
            this.valuesSources = null;
        }

        // FIXME: Hack
        this.formatter = configs.values().iterator().next().format();

        // derive the step from the rounding
        step = rounding.nextRoundingValue(rounding.round(0)) - rounding.round(0);

        bucketOrds = new LongHash(1, aggregationContext.bigArrays());

        if (valuesSources != null) {
            sums = context.bigArrays().newDoubleArray(1, true);
        }

        this.start = start;
        this.end = end;
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
                if (fieldVals.length != 3) {
                    throw new IllegalStateException("Invalid number of fields specified. Need 3, got " + fieldVals.length);
                }

                long rangeStartVal = 0;
                final NumericDoubleValues rangeStartDoubleValues = values[0];
                if (rangeStartDoubleValues.advanceExact(doc)) {
                    rangeStartVal = new Double(rangeStartDoubleValues.doubleValue()).longValue();
                }

                long rangeEndVal = 0;
                final NumericDoubleValues rangeEndDoubleValues = values[1];
                if (rangeEndDoubleValues.advanceExact(doc)) {
                    rangeEndVal = new Double(rangeEndDoubleValues.doubleValue()).longValue();
                }

                final Long rangeTotal = rangeEndVal - rangeStartVal;

                double valueVal = Double.NaN;
                final NumericDoubleValues valueDoubleValues = values[2];
                if (valueDoubleValues.advanceExact(doc)) {
                    valueVal = valueDoubleValues.doubleValue();
                }

                // round the first value
                long startRounded = rounding.round(Math.max(rangeStartVal, start) - offset) + offset;

                // round the last value
                long lastRounded = rounding.round(Math.min(rangeEndVal, end) - offset) + offset;

                //System.out.printf("%d has start: %d, end: %d and value: %f\n", doc,
                //        startRounded, lastRounded, valueVal);

                // add to all the buckets between first and last
                long bucketStart = startRounded;
                while (bucketStart <= lastRounded) {
                    long nextBucketStart = rounding.nextRoundingValue(bucketStart);

                    // calculate the ratio of time spent in this bucket
                    long timeInBucket = getTimeInWindow(bucketStart, nextBucketStart, rangeStartVal, rangeEndVal);
                    double bucketRatio = timeInBucket / rangeTotal.doubleValue();

                    // calculate the value that is proportional to the time spent in this bucket
                    double proportionalValue = valueVal * bucketRatio;

                    //System.out.printf("Adding doc %d to bucket: %d\n", doc, bucketStart);
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
        List<InternalTimeSliceHistogram.Bucket> buckets = new ArrayList<>((int) bucketOrds.size());
        for (long i = 0; i < bucketOrds.size(); i++) {
            final long bucketOrd = bucketOrds.get(i);
            buckets.add(new InternalTimeSliceHistogram.Bucket(bucketOrd, bucketDocCount(i), sums.get(i), keyed, formatter, bucketAggregations(i)));
        }

        // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
        CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator(this));

        // value source will be null for unmapped fields
        InternalTimeSliceHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalTimeSliceHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                : null;
        return new InternalTimeSliceHistogram(name, buckets, order, minDocCount, offset, emptyBucketInfo, formatter, keyed,
                pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalTimeSliceHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalTimeSliceHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                : null;
        return new InternalTimeSliceHistogram(name, Collections.emptyList(), order, minDocCount, offset, emptyBucketInfo, formatter, keyed,
                pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
        Releasables.close(sums);
    }
}
