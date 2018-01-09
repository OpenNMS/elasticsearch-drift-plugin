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

import static java.util.Collections.unmodifiableMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

public class TimeSliceAggregationBuilder extends MultiValuesSourceAggregationBuilder<ValuesSource.Numeric, TimeSliceAggregationBuilder>
        implements MultiBucketAggregationBuilder {
    public static final String NAME = "time_slice";

    public static final Map<String, DateTimeUnit> DATE_FIELD_UNITS;

    static {
        Map<String, DateTimeUnit> dateFieldUnits = new HashMap<>();
        dateFieldUnits.put("year", DateTimeUnit.YEAR_OF_CENTURY);
        dateFieldUnits.put("1y", DateTimeUnit.YEAR_OF_CENTURY);
        dateFieldUnits.put("quarter", DateTimeUnit.QUARTER);
        dateFieldUnits.put("1q", DateTimeUnit.QUARTER);
        dateFieldUnits.put("month", DateTimeUnit.MONTH_OF_YEAR);
        dateFieldUnits.put("1M", DateTimeUnit.MONTH_OF_YEAR);
        dateFieldUnits.put("week", DateTimeUnit.WEEK_OF_WEEKYEAR);
        dateFieldUnits.put("1w", DateTimeUnit.WEEK_OF_WEEKYEAR);
        dateFieldUnits.put("day", DateTimeUnit.DAY_OF_MONTH);
        dateFieldUnits.put("1d", DateTimeUnit.DAY_OF_MONTH);
        dateFieldUnits.put("hour", DateTimeUnit.HOUR_OF_DAY);
        dateFieldUnits.put("1h", DateTimeUnit.HOUR_OF_DAY);
        dateFieldUnits.put("minute", DateTimeUnit.MINUTES_OF_HOUR);
        dateFieldUnits.put("1m", DateTimeUnit.MINUTES_OF_HOUR);
        dateFieldUnits.put("second", DateTimeUnit.SECOND_OF_MINUTE);
        dateFieldUnits.put("1s", DateTimeUnit.SECOND_OF_MINUTE);
        DATE_FIELD_UNITS = unmodifiableMap(dateFieldUnits);
    }

    private long interval;
    private DateHistogramInterval dateHistogramInterval;
    private long offset = 0;
    private ExtendedBounds extendedBounds;
    private BucketOrder order = BucketOrder.key(true);
    private boolean keyed = false;
    private long minDocCount = 0;
    private long start = Long.MIN_VALUE;
    private long end = Long.MAX_VALUE;

    /** Create a new builder with the given name. */
    public TimeSliceAggregationBuilder(String name) {
        super(name, ValuesSourceType.NUMERIC, ValueType.DATE);
    }

    /** Read from a stream, for internal use only. */
    public TimeSliceAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.NUMERIC, ValueType.DATE);
        order = InternalOrder.Streams.readHistogramOrder(in, true);
        keyed = in.readBoolean();
        minDocCount = in.readVLong();
        interval = in.readLong();
        dateHistogramInterval = in.readOptionalWriteable(DateHistogramInterval::new);
        offset = in.readLong();
        extendedBounds = in.readOptionalWriteable(ExtendedBounds::new);
        start = in.readLong();
        end = in.readLong();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeHistogramOrder(order, out, true);
        out.writeBoolean(keyed);
        out.writeVLong(minDocCount);
        out.writeLong(interval);
        out.writeOptionalWriteable(dateHistogramInterval);
        out.writeLong(offset);
        out.writeOptionalWriteable(extendedBounds);
        out.writeLong(start);
        out.writeLong(end);
    }

    public TimeSliceAggregationBuilder start(long start) {
        this.start = start;
        return this;
    }

    public TimeSliceAggregationBuilder end(long end) {
        this.end = end;
        return this;
    }

    /** Get the current interval in milliseconds that is set on this builder. */
    public long interval() {
        return interval;
    }

    /** Set the interval on this builder, and return the builder so that calls can be chained.
     *  If both {@link #interval()} and {@link #dateHistogramInterval()} are set, then the
     *  {@link #dateHistogramInterval()} wins. */
    public TimeSliceAggregationBuilder interval(long interval) {
        if (interval < 1) {
            throw new IllegalArgumentException("[interval] must be 1 or greater for histogram aggregation [" + name + "]");
        }
        this.interval = interval;
        return this;
    }

    /** Get the current date interval that is set on this builder. */
    public DateHistogramInterval dateHistogramInterval() {
        return dateHistogramInterval;
    }

    /** Set the interval on this builder, and return the builder so that calls can be chained.
     *  If both {@link #interval()} and {@link #dateHistogramInterval()} are set, then the
     *  {@link #dateHistogramInterval()} wins. */
    public TimeSliceAggregationBuilder dateHistogramInterval(DateHistogramInterval dateHistogramInterval) {
        if (dateHistogramInterval == null) {
            throw new IllegalArgumentException("[dateHistogramInterval] must not be null: [" + name + "]");
        }
        this.dateHistogramInterval = dateHistogramInterval;
        return this;
    }

    /** Get the offset to use when rounding, which is a number of milliseconds. */
    public long offset() {
        return offset;
    }

    /** Set the offset on this builder, which is a number of milliseconds, and
     *  return the builder so that calls can be chained. */
    public TimeSliceAggregationBuilder offset(long offset) {
        this.offset = offset;
        return this;
    }

    /** Set the offset on this builder, as a time value, and
     *  return the builder so that calls can be chained. */
    public TimeSliceAggregationBuilder offset(String offset) {
        if (offset == null) {
            throw new IllegalArgumentException("[offset] must not be null: [" + name + "]");
        }
        return offset(parseStringOffset(offset));
    }

    static long parseStringOffset(String offset) {
        if (offset.charAt(0) == '-') {
            return -TimeValue
                    .parseTimeValue(offset.substring(1), null, TimeSliceAggregationBuilder.class.getSimpleName() + ".parseOffset")
                    .millis();
        }
        int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue
                .parseTimeValue(offset.substring(beginIndex), null, TimeSliceAggregationBuilder.class.getSimpleName() + ".parseOffset")
                .millis();
    }

    /** Return extended bounds for this histogram, or {@code null} if none are set. */
    public ExtendedBounds extendedBounds() {
        return extendedBounds;
    }

    /** Set extended bounds on this histogram, so that buckets would also be
     *  generated on intervals that did not match any documents. */
    public TimeSliceAggregationBuilder extendedBounds(ExtendedBounds extendedBounds) {
        if (extendedBounds == null) {
            throw new IllegalArgumentException("[extendedBounds] must not be null: [" + name + "]");
        }
        this.extendedBounds = extendedBounds;
        return this;
    }

    /** Return the order to use to sort buckets of this histogram. */
    public BucketOrder order() {
        return order;
    }

    /** Set a new order on this builder and return the builder so that calls
     *  can be chained. A tie-breaker may be added to avoid non-deterministic ordering. */
    public TimeSliceAggregationBuilder order(BucketOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        if(order instanceof InternalOrder.CompoundOrder || InternalOrder.isKeyOrder(order)) {
            this.order = order; // if order already contains a tie-breaker we are good to go
        } else { // otherwise add a tie-breaker by using a compound order
            this.order = BucketOrder.compound(order);
        }
        return this;
    }

    /**
     * Sets the order in which the buckets will be returned. A tie-breaker may be added to avoid non-deterministic
     * ordering.
     */
    public TimeSliceAggregationBuilder order(List<BucketOrder> orders) {
        if (orders == null) {
            throw new IllegalArgumentException("[orders] must not be null: [" + name + "]");
        }
        // if the list only contains one order use that to avoid inconsistent xcontent
        order(orders.size() > 1 ? BucketOrder.compound(orders) : orders.get(0));
        return this;
    }

    /** Return whether buckets should be returned as a hash. In case
     *  {@code keyed} is false, buckets will be returned as an array. */
    public boolean keyed() {
        return keyed;
    }

    /** Set whether to return buckets as a hash or as an array, and return the
     *  builder so that calls can be chained. */
    public TimeSliceAggregationBuilder keyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    /** Return the minimum count of documents that buckets need to have in order
     *  to be included in the response. */
    public long minDocCount() {
        return minDocCount;
    }

    /** Set the minimum count of matching documents that buckets need to have
     *  and return this builder so that calls can be chained. */
    public TimeSliceAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                    "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]");
        }
        this.minDocCount = minDocCount;
        return this;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {

        if (dateHistogramInterval == null) {
            builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), interval);
        } else {
            builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), dateHistogramInterval.toString());
        }
        builder.field(Histogram.OFFSET_FIELD.getPreferredName(), offset);

        if (order != null) {
            builder.field(Histogram.ORDER_FIELD.getPreferredName());
            order.toXContent(builder, params);
        }

        builder.field(Histogram.KEYED_FIELD.getPreferredName(), keyed);

        builder.field(Histogram.MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);

        if (extendedBounds != null) {
            extendedBounds.toXContent(builder, params);
        }

        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected TimeSliceAggregatorFactory innerBuild(SearchContext context, Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs, AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        Rounding rounding = createRounding();
        ExtendedBounds roundedBounds = null;
        return new TimeSliceAggregatorFactory(name, configs, interval, dateHistogramInterval, offset, order, keyed, minDocCount,
                rounding, roundedBounds, context, parent, subFactoriesBuilder, metaData, start, end);
    }

    @Override
    protected Map<String, ValuesSourceConfig<ValuesSource.Numeric>> resolveConfig(SearchContext context) {
        // Use a LinkedHashMap here to preserve ordering
        Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs = new LinkedHashMap<>();
        for (String field : fields()) {
            ValuesSourceConfig<ValuesSource.Numeric> config = config(context, field, null);
            configs.put(field, config);
        }
        return configs;
    }

    private Rounding createRounding() {
        Rounding.Builder tzRoundingBuilder;
        if (dateHistogramInterval != null) {
            DateTimeUnit dateTimeUnit = DATE_FIELD_UNITS.get(dateHistogramInterval.toString());
            if (dateTimeUnit != null) {
                tzRoundingBuilder = Rounding.builder(dateTimeUnit);
            } else {
                // the interval is a time value?
                tzRoundingBuilder = Rounding.builder(
                        TimeValue.parseTimeValue(dateHistogramInterval.toString(), null, getClass().getSimpleName() + ".interval"));
            }
        } else {
            // the interval is an integer time value in millis?
            tzRoundingBuilder = Rounding.builder(TimeValue.timeValueMillis(interval));
        }
        //if (timeZone() != null) {
         //   tzRoundingBuilder.timeZone(timeZone());
        //}
        Rounding rounding = tzRoundingBuilder.build();
        return rounding;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(order, keyed, minDocCount, interval, dateHistogramInterval, minDocCount, extendedBounds);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        TimeSliceAggregationBuilder other = (TimeSliceAggregationBuilder) obj;
        return Objects.equals(order, other.order)
                && Objects.equals(keyed, other.keyed)
                && Objects.equals(minDocCount, other.minDocCount)
                && Objects.equals(interval, other.interval)
                && Objects.equals(dateHistogramInterval, other.dateHistogramInterval)
                && Objects.equals(offset, other.offset)
                && Objects.equals(extendedBounds, other.extendedBounds);
    }
}
