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
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

/**
 * This is a copy of {@link org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder}
 * with the following changes.
 *
 * 1) We include extra start and end fields that allow the aggregation to be limited to a smaller range than the
 * actual values represent.
 * 2) We use multiple fields instead of a single field, since the user needs to specify the values for
 * the range start/end along with the field for the actual value.
 */
public class ProportionalSumAggregationBuilder extends MultiValuesSourceAggregationBuilder<ValuesSource.Numeric, ProportionalSumAggregationBuilder>
        implements MultiBucketAggregationBuilder {

    public static final String NAME = "proportional_sum";

    private long interval;
    private DateHistogramInterval dateHistogramInterval;
    private Long offset;
    private ExtendedBounds extendedBounds;
    private BucketOrder order = BucketOrder.key(true);
    private boolean keyed = false;
    private long minDocCount = 0;
    private Long start;
    private Long end;

    /** Create a new builder with the given name. */
    public ProportionalSumAggregationBuilder(String name) {
        super(name, ValuesSourceType.NUMERIC, ValueType.DATE);
    }

    /** Read from a stream, for internal use only. */
    public ProportionalSumAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.NUMERIC, ValueType.DATE);
        order = InternalOrder.Streams.readHistogramOrder(in, true);
        keyed = in.readBoolean();
        minDocCount = in.readVLong();
        interval = in.readLong();
        dateHistogramInterval = in.readOptionalWriteable(DateHistogramInterval::new);
        offset = in.readOptionalLong();
        extendedBounds = in.readOptionalWriteable(ExtendedBounds::new);
        start = in.readOptionalLong();
        end = in.readOptionalLong();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeHistogramOrder(order, out, true);
        out.writeBoolean(keyed);
        out.writeVLong(minDocCount);
        out.writeLong(interval);
        out.writeOptionalWriteable(dateHistogramInterval);
        out.writeOptionalLong(offset);
        out.writeOptionalWriteable(extendedBounds);
        out.writeOptionalLong(start);
        out.writeOptionalLong(end);
    }

    /** Get the current start in milliseconds that is set on this builder. */
    public Long start() { return start; }

    /** Set the start on this builder, which is a number of milliseconds, and
     *  return the builder so that calls can be chained. */
    public ProportionalSumAggregationBuilder start(Long start) {
        this.start = start;
        return this;
    }

    /** Get the current end in milliseconds that is set on this builder. */
    public Long end() { return start; }

    /** Set the end on this builder, which is a number of milliseconds, and
     *  return the builder so that calls can be chained. */
    public ProportionalSumAggregationBuilder end(Long end) {
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
    public ProportionalSumAggregationBuilder interval(long interval) {
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
    public ProportionalSumAggregationBuilder dateHistogramInterval(DateHistogramInterval dateHistogramInterval) {
        if (dateHistogramInterval == null) {
            throw new IllegalArgumentException("[dateHistogramInterval] must not be null: [" + name + "]");
        }
        this.dateHistogramInterval = dateHistogramInterval;
        return this;
    }

    /** Get the offset to use when rounding, which is a number of milliseconds. */
    public Long offset() {
        return offset;
    }

    /** Set the offset on this builder, which is a number of milliseconds, and
     *  return the builder so that calls can be chained. */
    public ProportionalSumAggregationBuilder offset(Long offset) {
        this.offset = offset;
        return this;
    }

    /** Set the offset on this builder, as a time value, and
     *  return the builder so that calls can be chained. */
    public ProportionalSumAggregationBuilder offset(String offset) {
        if (offset == null) {
            this.offset = null;
            return this;
        }
        return offset(parseStringOffset(offset));
    }

    static long parseStringOffset(String offset) {
        if (offset.charAt(0) == '-') {
            return -TimeValue
                    .parseTimeValue(offset.substring(1), null, ProportionalSumAggregationBuilder.class.getSimpleName() + ".parseOffset")
                    .millis();
        }
        int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue
                .parseTimeValue(offset.substring(beginIndex), null, ProportionalSumAggregationBuilder.class.getSimpleName() + ".parseOffset")
                .millis();
    }

    /** Return extended bounds for this histogram, or {@code null} if none are set. */
    public ExtendedBounds extendedBounds() {
        return extendedBounds;
    }

    /** Set extended bounds on this histogram, so that buckets would also be
     *  generated on intervals that did not match any documents. */
    public ProportionalSumAggregationBuilder extendedBounds(ExtendedBounds extendedBounds) {
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
    public ProportionalSumAggregationBuilder order(BucketOrder order) {
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
    public ProportionalSumAggregationBuilder order(List<BucketOrder> orders) {
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
    public ProportionalSumAggregationBuilder keyed(boolean keyed) {
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
    public ProportionalSumAggregationBuilder minDocCount(long minDocCount) {
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

        if (offset != null) {
            builder.field(Histogram.OFFSET_FIELD.getPreferredName(), offset);
        }

        if (order != null) {
            builder.field(Histogram.ORDER_FIELD.getPreferredName());
            order.toXContent(builder, params);
        }

        builder.field(Histogram.KEYED_FIELD.getPreferredName(), keyed);

        builder.field(Histogram.MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);

        if (extendedBounds != null) {
            extendedBounds.toXContent(builder, params);
        }

        if (start != null) {
            builder.field(ProportionalSumParser.START_FIELD.getPreferredName(), start);
        }
        if (end != null) {
            builder.field(ProportionalSumParser.END_FIELD.getPreferredName(), start);
        }

        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    protected static ValuesSourceConfig<ValuesSource.Numeric> getValueSourceForRangeStart(Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs) {
        final int numActualConfigs = configs != null ? configs.size() : 0;
        if (numActualConfigs != 3) {
            throw new IllegalArgumentException(
                    "[values] must contain 3 values. Found [" + numActualConfigs + "].");
        }
        return configs.values().iterator().next();
    }

    @Override
    protected ProportionalSumAggregatorFactory innerBuild(SearchContext context, Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs, AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        Rounding rounding = createRounding(configs);
        ExtendedBounds roundedBounds = null;
        if (this.extendedBounds != null) {
            DocValueFormat format = getValueSourceForRangeStart(configs).format();
            // parse any string bounds to longs and round
            roundedBounds = this.extendedBounds.parseAndValidate(name, context, format).round(rounding);
        }
        long effectiveOffset = 0;
        if (offset != null) {
            effectiveOffset = offset;
        } else if (start != null) {
            final long delta = start - rounding.round(start);
            if (delta > 0) {
                effectiveOffset = delta;
            }
        }
        return new ProportionalSumAggregatorFactory(name, configs, effectiveOffset, order, keyed, minDocCount,
                rounding, roundedBounds, context, parent, subFactoriesBuilder, metaData, start, end);
    }

    @Override
    protected Map<String, ValuesSourceConfig<ValuesSource.Numeric>> resolveConfig(SearchContext context) {
        // HACK: Use a LinkedHashMap here to preserve ordering
        Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs = new LinkedHashMap<>();
        for (String field : fields()) {
            ValuesSourceConfig<ValuesSource.Numeric> config = config(context, field, null);
            configs.put(field, config);
        }
        return configs;
    }

    private Rounding createRounding(Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs) {
        Rounding.Builder tzRoundingBuilder;
        if (dateHistogramInterval != null) {
            DateTimeUnit dateTimeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(dateHistogramInterval.toString());
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
        if (getValueSourceForRangeStart(configs).timezone() != null) {
           tzRoundingBuilder.timeZone(getValueSourceForRangeStart(configs).timezone());
        }
        return tzRoundingBuilder.build();
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(order, keyed, minDocCount, interval, dateHistogramInterval,
                minDocCount, extendedBounds, start, end);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        ProportionalSumAggregationBuilder other = (ProportionalSumAggregationBuilder) obj;
        return Objects.equals(order, other.order)
                && Objects.equals(keyed, other.keyed)
                && Objects.equals(minDocCount, other.minDocCount)
                && Objects.equals(interval, other.interval)
                && Objects.equals(dateHistogramInterval, other.dateHistogramInterval)
                && Objects.equals(offset, other.offset)
                && Objects.equals(extendedBounds, other.extendedBounds)
                && Objects.equals(start, other.start)
                && Objects.equals(end, other.end);
    }
}
