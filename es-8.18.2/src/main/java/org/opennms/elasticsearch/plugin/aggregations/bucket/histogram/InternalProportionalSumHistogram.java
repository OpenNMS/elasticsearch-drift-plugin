/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.opennms.elasticsearch.plugin.aggregations.bucket.histogram;

import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.LongBounds;
import org.elasticsearch.xcontent.XContentBuilder;
import java.time.Instant;
import java.time.ZoneId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;

/**
 * Copy of {@link org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram} with the addition
 * of each bucket having a (double) value.
 */
public final class InternalProportionalSumHistogram extends InternalMultiBucketAggregation<InternalProportionalSumHistogram, InternalProportionalSumHistogram.Bucket>
        implements Histogram, HistogramFactory {

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Histogram.Bucket, HistogramBucketWithValue, KeyComparable<Bucket> {

        final long key;
        final long docCount;
        final double value;
        final InternalAggregations aggregations;
        private final transient boolean keyed;
        protected final transient DocValueFormat format;

        public Bucket(long key, long docCount, double value, boolean keyed, DocValueFormat format,
                      InternalAggregations aggregations) {
            this.format = format;
            this.value = value;
            this.keyed = keyed;
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, boolean keyed, DocValueFormat format) throws IOException {
            this.format = format;
            this.keyed = keyed;
            key = in.readLong();
            docCount = in.readVLong();
            value = in.readDouble();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != InternalProportionalSumHistogram.Bucket.class) {
                return false;
            }
            InternalProportionalSumHistogram.Bucket that = (InternalProportionalSumHistogram.Bucket) obj;
            // No need to take the keyed and format parameters into account,
            // they are already stored and tested on the InternalDateHistogram object
            return key == that.key
                    && docCount == that.docCount
                    && value == that.value
                    && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, docCount, aggregations, value);
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(key);
            out.writeVLong(docCount);
            out.writeDouble(value);
            aggregations.writeTo(out);
        }

        @Override
        public String getKeyAsString() {
            return format.format(key).toString();
        }

        @Override
        public Object getKey() {
            return Instant.ofEpochMilli(key);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        Bucket reduce(List<Bucket> buckets, AggregationReduceContext context) {
            List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
            long docCount = 0;
            double value = 0;
            for (Bucket bucket : buckets) {
                docCount += bucket.docCount;
                if (!Double.isNaN(bucket.value)) {
                    value += bucket.value;
                }
                aggregations.add((InternalAggregations) bucket.getAggregations());
            }
            InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
            return new InternalProportionalSumHistogram.Bucket(key, docCount, value, keyed, format, aggs);
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String keyAsString = format.format(key).toString();
            if (keyed) {
                builder.startObject(keyAsString);
            } else {
                builder.startObject();
            }
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), keyAsString);
            }
            builder.field(CommonFields.KEY.getPreferredName(), key);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            builder.field(CommonFields.VALUE.getPreferredName(), value);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int compareKey(Bucket other) {
            return Long.compare(key, other.key);
        }

        public DocValueFormat getFormatter() {
            return format;
        }

        public boolean getKeyed() {
            return keyed;
        }

        @Override
        public double getValue() {
            return value;
        }
    }

    static class EmptyBucketInfo {

        final Rounding rounding;
        final InternalAggregations subAggregations;
        final LongBounds bounds;

        EmptyBucketInfo(Rounding rounding, InternalAggregations subAggregations) {
            this(rounding, subAggregations, null);
        }

        EmptyBucketInfo(Rounding rounding, InternalAggregations subAggregations, LongBounds bounds) {
            this.rounding = rounding;
            this.subAggregations = subAggregations;
            this.bounds = bounds;
        }

        EmptyBucketInfo(StreamInput in) throws IOException {
            rounding = Rounding.read(in);
            subAggregations = InternalAggregations.readFrom(in);
            bounds = in.readOptionalWriteable(LongBounds::new);
        }

        void writeTo(StreamOutput out) throws IOException {
            rounding.writeTo(out);
            subAggregations.writeTo(out);
            out.writeOptionalWriteable(bounds);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            EmptyBucketInfo that = (EmptyBucketInfo) obj;
            return Objects.equals(rounding, that.rounding)
                    && Objects.equals(bounds, that.bounds)
                    && Objects.equals(subAggregations, that.subAggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), rounding, bounds, subAggregations);
        }
    }

    private final List<Bucket> buckets;
    private final BucketOrder order;
    private final DocValueFormat format;
    private final boolean keyed;
    private final long minDocCount;
    private final long offset;
    private final EmptyBucketInfo emptyBucketInfo;

    InternalProportionalSumHistogram(String name, List<Bucket> buckets, BucketOrder order, long minDocCount, long offset,
                                     EmptyBucketInfo emptyBucketInfo,
                                     DocValueFormat formatter, boolean keyed,
                                     Map<String, Object> metaData) {
        super(name, metaData);
        this.buckets = buckets;
        this.order = order;
        this.offset = offset;
        assert (minDocCount == 0) == (emptyBucketInfo != null);
        this.minDocCount = minDocCount;
        this.emptyBucketInfo = emptyBucketInfo;
        this.format = formatter;
        this.keyed = keyed;
    }

    /**
     * Stream from a stream.
     */
    public InternalProportionalSumHistogram(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readHistogramOrder(in);
        minDocCount = in.readVLong();
        if (minDocCount == 0) {
            emptyBucketInfo = new EmptyBucketInfo(in);
        } else {
            emptyBucketInfo = null;
        }
        offset = in.readLong();
        format = in.readNamedWriteable(DocValueFormat.class);
        keyed = in.readBoolean();
        buckets = in.readCollectionAsList(stream -> new Bucket(stream, keyed, format));
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeHistogramOrder(order, out);
        out.writeVLong(minDocCount);
        if (minDocCount == 0) {
            emptyBucketInfo.writeTo(out);
        }
        out.writeLong(offset);
        out.writeNamedWriteable(format);
        out.writeBoolean(keyed);
        out.writeCollection(buckets, (o, b) -> b.writeTo(o));
    }

    @Override
    public String getWriteableName() {
        return ProportionalSumAggregationBuilder.NAME;
    }

    @Override
    public List<InternalProportionalSumHistogram.Bucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    DocValueFormat getFormatter() {
        return format;
    }

    long getMinDocCount() {
        return minDocCount;
    }

    long getOffset() {
        return offset;
    }

    BucketOrder getOrder() {
        return order;
    }

    @Override
    public InternalProportionalSumHistogram create(List<Bucket> buckets) {
        return new InternalProportionalSumHistogram(name, buckets, order, minDocCount, offset, emptyBucketInfo, format,
                keyed, metadata);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, prototype.value, prototype.keyed, prototype.format, aggregations);
    }

    /**
     * Reduce a list of same-keyed buckets (from multiple shards) to a single bucket. This
     * requires all buckets to have the same key.
     */
    protected Bucket reduceBucket(List<Bucket> buckets, AggregationReduceContext context) {
        List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
        long docCount = 0;
        double value = 0;
        for (Bucket bucket : buckets) {
            docCount += bucket.docCount;
            if (!Double.isNaN(bucket.value)) {
                value += bucket.value;
            }
            aggregations.add((InternalAggregations) bucket.getAggregations());
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
        return new InternalProportionalSumHistogram.Bucket(buckets.get(0).key, docCount, value, keyed, format, aggs);
    }

    private static class IteratorAndCurrent {

        private final Iterator<Bucket> iterator;
        private Bucket current;

        IteratorAndCurrent(Iterator<Bucket> iterator) {
            this.iterator = iterator;
            current = iterator.next();
        }

    }

    private List<Bucket> reduceBuckets(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {

        final PriorityQueue<IteratorAndCurrent> pq = new PriorityQueue<IteratorAndCurrent>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent a, IteratorAndCurrent b) {
                return a.current.key < b.current.key;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            InternalProportionalSumHistogram histogram = (InternalProportionalSumHistogram) aggregation;
            if (histogram.buckets.isEmpty() == false) {
                pq.add(new IteratorAndCurrent(histogram.buckets.iterator()));
            }
        }

        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same key
            List<Bucket> currentBuckets = new ArrayList<>();
            double key = pq.top().current.key;

            do {
                final IteratorAndCurrent top = pq.top();

                if (top.current.key != key) {
                    // the key changes, reduce what we already buffered and reset the buffer for current buckets
                    final Bucket reduced = currentBuckets.get(0).reduce(currentBuckets, reduceContext);
                    if (reduced.getDocCount() >= minDocCount || reduceContext.isFinalReduce() == false) {
                        reduceContext.consumeBucketsAndMaybeBreak(1);
                        reducedBuckets.add(reduced);
                    } else {
                        reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(reduced));
                    }
                    currentBuckets.clear();
                    key = top.current.key;
                }

                currentBuckets.add(top.current);

                if (top.iterator.hasNext()) {
                    final Bucket next = top.iterator.next();
                    assert next.key > top.current.key : "shards must return data sorted by key";
                    top.current = next;
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final Bucket reduced = currentBuckets.get(0).reduce(currentBuckets, reduceContext);
                if (reduced.getDocCount() >= minDocCount || reduceContext.isFinalReduce() == false) {
                    reduceContext.consumeBucketsAndMaybeBreak(1);
                    reducedBuckets.add(reduced);
                } else {
                    reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(reduced));
                }
            }
        }

        return reducedBuckets;
    }

    private void addEmptyBuckets(List<Bucket> list, AggregationReduceContext reduceContext) {
        Bucket lastBucket = null;
        LongBounds bounds = emptyBucketInfo.bounds;
        ListIterator<Bucket> iter = list.listIterator();

        // first adding all the empty buckets *before* the actual data (based on th extended_bounds.min the user requested)
        InternalAggregations reducedEmptySubAggs = InternalAggregations.reduce(Collections.singletonList(emptyBucketInfo.subAggregations),
                reduceContext);
        if (bounds != null) {
            Bucket firstBucket = iter.hasNext() ? list.get(iter.nextIndex()) : null;
            if (firstBucket == null) {
                if (bounds.getMin() != null && bounds.getMax() != null) {
                    long key = bounds.getMin() + offset;
                    long max = bounds.getMax() + offset;
                    while (key <= max) {
                        reduceContext.consumeBucketsAndMaybeBreak(1);
                        iter.add(new InternalProportionalSumHistogram.Bucket(key, 0, 0, keyed, format, reducedEmptySubAggs));
                        key = nextKey(key).longValue();
                    }
                }
            } else {
                if (bounds.getMin() != null) {
                    long key = bounds.getMin() + offset;
                    if (key < firstBucket.key) {
                        while (key < firstBucket.key) {
                            reduceContext.consumeBucketsAndMaybeBreak(1);
                            iter.add(new InternalProportionalSumHistogram.Bucket(key, 0, 0, keyed, format, reducedEmptySubAggs));
                            key = nextKey(key).longValue();
                        }
                    }
                }
            }
        }

        // now adding the empty buckets within the actual data,
        // e.g. if the data series is [1,2,3,7] there're 3 empty buckets that will be created for 4,5,6
        while (iter.hasNext()) {
            Bucket nextBucket = list.get(iter.nextIndex());
            if (lastBucket != null) {
                long key = nextKey(lastBucket.key).longValue();
                while (key < nextBucket.key) {
                    reduceContext.consumeBucketsAndMaybeBreak(1);
                    iter.add(new InternalProportionalSumHistogram.Bucket(key, 0, 0, keyed, format, reducedEmptySubAggs));
                    key = nextKey(key).longValue();
                }
                assert key == nextBucket.key : "key: " + key + ", nextBucket.key: " + nextBucket.key;
            }
            lastBucket = iter.next();
        }

        // finally, adding the empty buckets *after* the actual data (based on the extended_bounds.max requested by the user)
        if (bounds != null && lastBucket != null && bounds.getMax() != null && bounds.getMax() + offset > lastBucket.key) {
            long key = nextKey(lastBucket.key).longValue();
            long max = bounds.getMax() + offset;
            while (key <= max) {
                reduceContext.consumeBucketsAndMaybeBreak(1);
                iter.add(new InternalProportionalSumHistogram.Bucket(key, 0, 0, keyed, format, reducedEmptySubAggs));
                key = nextKey(key).longValue();
            }
        }
    }

    @Override
    public AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            final List<InternalAggregation> aggregations = new ArrayList<>(size);

            @Override
            public void accept(InternalAggregation aggregation) {
                aggregations.add(aggregation);
            }

            @Override
            public InternalAggregation get() {
                return doReduce(aggregations, reduceContext);
            }
        };
    }

    private InternalAggregation doReduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        List<Bucket> reducedBuckets = reduceBuckets(aggregations, reduceContext);

        // adding empty buckets if needed
        if (minDocCount == 0) {
            addEmptyBuckets(reducedBuckets, reduceContext);
        }

        if (InternalOrder.isKeyAsc(order) || reduceContext.isFinalReduce() == false) {
            // nothing to do, data are already sorted since shards return
            // sorted buckets and the merge-sort performed by reduceBuckets
            // maintains order
        } else if (InternalOrder.isKeyDesc(order)) {
            // we just need to reverse here...
            List<Bucket> reverse = new ArrayList<>(reducedBuckets);
            Collections.reverse(reverse);
            reducedBuckets = reverse;
        } else {
            // sorted by compound order or sub-aggregation, need to fall back to a costly n*log(n) sort
            CollectionUtil.introSort(reducedBuckets, order.comparator());
        }

        return new InternalProportionalSumHistogram(getName(), reducedBuckets, order, minDocCount, offset, emptyBucketInfo,
                format, keyed, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    // HistogramFactory method impls

    public Number getKey(MultiBucketsAggregation.Bucket bucket) {
        return ((Bucket) bucket).key;
    }

    public Number nextKey(Number key) {
        return emptyBucketInfo.rounding.nextRoundingValue(key.longValue() - offset) + offset;
    }

    public InternalAggregation createAggregation(List<MultiBucketsAggregation.Bucket> buckets) {
        // convert buckets to the right type
        List<Bucket> buckets2 = new ArrayList<>(buckets.size());
        for (Object b : buckets) {
            buckets2.add((Bucket) b);
        }
        buckets2 = Collections.unmodifiableList(buckets2);
        return new InternalProportionalSumHistogram(name, buckets2, order, minDocCount, offset, emptyBucketInfo, format,
                keyed, getMetadata());
    }

    @Override
    public Bucket createBucket(Number key, long docCount, InternalAggregations aggregations) {
        return new Bucket(key.longValue(), docCount, 0, keyed, format, aggregations);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalProportionalSumHistogram that = (InternalProportionalSumHistogram) obj;
        return Objects.equals(buckets, that.buckets)
                && Objects.equals(order, that.order)
                && Objects.equals(format, that.format)
                && Objects.equals(keyed, that.keyed)
                && Objects.equals(minDocCount, that.minDocCount)
                && Objects.equals(offset, that.offset)
                && Objects.equals(emptyBucketInfo, that.emptyBucketInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, order, format, keyed, minDocCount, offset, emptyBucketInfo);
    }
}
