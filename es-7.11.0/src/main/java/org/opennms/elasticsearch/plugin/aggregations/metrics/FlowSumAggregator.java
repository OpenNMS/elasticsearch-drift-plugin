/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.opennms.elasticsearch.plugin.aggregations.metrics;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.metrics.InternalSum;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.opennms.elasticsearch.plugin.aggregations.bucket.histogram.FlowHistogramAggregator;

/**
 * Based on org.elasticsearch.search.aggregations.metrics.SumAggregator (from ES 7.11.0).
 */
public class FlowSumAggregator extends NumericMetricsAggregator.SingleValue {

    public static double bytesInBucket(
            long deltaSwitched,
            long lastSwitchedInclusive,
            double multipliedNumBytes,
            double windowStart,
            double windowEndInclusive
    ) {
        // The flow duration ranges [delta_switched, last_switched] (both bounds are inclusive)
        long flowDurationMs = lastSwitchedInclusive - deltaSwitched + 1;

        // the start (inclusive) of the flow in this window
        double overlapStart = Math.max(deltaSwitched, windowStart);
        // the end (inclusive) of the flow in this window
        double overlapEnd = Math.min(lastSwitchedInclusive, windowEndInclusive);

        // the end of the previous window (inclusive)
        double previousEnd = overlapStart - 1;

        double bytesAtPreviousEnd = ((previousEnd - deltaSwitched + 1) * multipliedNumBytes / flowDurationMs);
        double bytesAtEnd = ((overlapEnd - deltaSwitched + 1) * multipliedNumBytes / flowDurationMs);

        return bytesAtEnd - bytesAtPreviousEnd;
    }

    private final ValuesSource.Numeric valuesSource;
    private final ValuesSource.Numeric flowStartSource;
    private final ValuesSource.Numeric flowEndSource;
    private final ValuesSource.Numeric samplingSource;
    private final DocValueFormat format;

    private DoubleArray sums;
    private DoubleArray compensations;

    private final FlowHistogramAggregator flowHistogramAggregator;

    FlowSumAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        ValuesSourceConfig rangeStartSourceConfig,
        ValuesSourceConfig rangeEndSourceConfig,
        ValuesSourceConfig samplingSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : ValuesSource.Numeric.EMPTY;
        this.flowStartSource = rangeStartSourceConfig.hasValues() ? (ValuesSource.Numeric) rangeStartSourceConfig.getValuesSource() : ValuesSource.Numeric.EMPTY;
        this.flowEndSource = rangeEndSourceConfig.hasValues() ? (ValuesSource.Numeric) rangeEndSourceConfig.getValuesSource() : ValuesSource.Numeric.EMPTY;
        this.samplingSource = samplingSourceConfig.hasValues() ? (ValuesSource.Numeric) samplingSourceConfig.getValuesSource() : ValuesSource.Numeric.EMPTY;
        this.format = valuesSourceConfig.format();
        if (valuesSource != null) {
            sums = bigArrays().newDoubleArray(1, true);
            compensations = bigArrays().newDoubleArray(1, true);
        }
        this.flowHistogramAggregator = (FlowHistogramAggregator)parent;
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedNumericDocValues values = valuesSource.longValues(ctx);
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                sums = bigArrays().grow(sums, bucket + 1);
                compensations = bigArrays().grow(compensations, bucket + 1);

                if (values.advanceExact(doc)) {

                    // start of the range
                    final SortedNumericDocValues rangeStartValues = flowStartSource.longValues(ctx);
                    long flowStartIncl = 0;
                    if (rangeStartValues.advanceExact(doc)) {
                        flowStartIncl = rangeStartValues.nextValue();
                    }
                    if (flowStartIncl < 0) {
                        throw new IllegalArgumentException("Invalid range start: " + flowStartIncl);
                    }

                    // end of the range
                    final SortedNumericDocValues rangeEndValues = flowEndSource.longValues(ctx);
                    long flowEndIncl = 0;
                    if (rangeEndValues.advanceExact(doc)) {
                        flowEndIncl = rangeEndValues.nextValue();
                    }
                    if (flowEndIncl < 0) {
                        throw new IllegalArgumentException("Invalid range end: " + flowEndIncl);
                    }
                    if (flowEndIncl < flowStartIncl) {
                        throw new IllegalArgumentException("Start cannot be after end! start: " +
                                                           flowStartIncl + " end: " + flowEndIncl);
                    }

                    // scale value by sampling interval
                    final SortedNumericDoubleValues samplingDoubleValues = samplingSource.doubleValues(ctx);
                    double sampling = 1.0;
                    if (samplingDoubleValues.advanceExact(doc)) {
                        final Double samplingValue = samplingDoubleValues.nextValue();
                        if (Double.isFinite(samplingValue) && samplingValue != 0.0) {
                            sampling = samplingValue;
                        }
                    }

                    final int valuesCount = values.docValueCount();
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    double sum = sums.get(bucket);
                    double compensation = compensations.get(bucket);
                    kahanSummation.reset(sum, compensation);

                    for (int i = 0; i < valuesCount; i++) {
                        long bytesInFlow = values.nextValue();

                        // determine the proportion of the value that falls into the bucket

                        // the key of the current bucket is the interval number of the corresponding interval
                        double bucketKey = flowHistogramAggregator.getBucketKey(bucket);
                        // interval and offset are not set at construction time in the parent range aggregation
                        double interval = flowHistogramAggregator.getInterval();
                        double offset = flowHistogramAggregator.getOffset();

                        double bucketStartIncl = bucketKey * interval + offset;
                        double bucketEndIncl = bucketStartIncl + interval - 1;

                        double bytesInBucket = bytesInBucket(
                                flowStartIncl,
                                flowEndIncl,
                                bytesInFlow * sampling,
                                bucketStartIncl,
                                bucketEndIncl
                        );

                        kahanSummation.add(bytesInBucket);
                    }

                    compensations.set(bucket, kahanSummation.delta());
                    sums.set(bucket, kahanSummation.value());
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= sums.size()) {
            return 0.0;
        }
        return sums.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalSum(name, sums.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSum(name, 0.0, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(sums, compensations);
    }

}
