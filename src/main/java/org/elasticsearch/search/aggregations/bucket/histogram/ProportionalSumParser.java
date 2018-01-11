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
package org.elasticsearch.search.aggregations.bucket.histogram;

import static org.elasticsearch.search.aggregations.bucket.histogram.Histogram.INTERVAL_FIELD;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

/**
 * Copy of {@link org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsParser} extended
 * to support the fields used is in the ProportionalSumAggregationBuilder.
 *
 * Ideally we would use a {@link org.elasticsearch.common.xcontent.ObjectParser} instead of this class, similar
 * to what is already implemented in the {@link org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder},
 * but the MultiValuesSource classes don't support this yet.
 *
 */
public class ProportionalSumParser extends MultiValuesSourceParser.NumericValuesSourceParser {

    protected static ParseField START_FIELD = new ParseField("start");
    protected static ParseField END_FIELD = new ParseField("end");

    public ProportionalSumParser() {
        super(true);
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, XContentParser.Token token, XContentParser parser, Map<ParseField, Object> otherOptions) throws IOException {
        if (INTERVAL_FIELD.match(currentFieldName)) {
            if (token == XContentParser.Token.VALUE_NUMBER) {
                otherOptions.put(INTERVAL_FIELD, parser.longValue());
                return true;
            } else if (token == XContentParser.Token.VALUE_STRING) {
                otherOptions.put(INTERVAL_FIELD, new DateHistogramInterval(parser.text()));
                return true;
            }
        } else if (Histogram.OFFSET_FIELD.match(currentFieldName)) {
            if (token == XContentParser.Token.VALUE_NUMBER) {
                otherOptions.put(Histogram.OFFSET_FIELD, parser.longValue());
                return true;
            } else {
                otherOptions.put(Histogram.OFFSET_FIELD, DateHistogramAggregationBuilder.parseStringOffset(parser.text()));
                return true;
            }
        } else if (Histogram.KEYED_FIELD.match(currentFieldName)) {
            if (token == XContentParser.Token.VALUE_BOOLEAN) {
                otherOptions.put(Histogram.KEYED_FIELD, parser.booleanValue());
                return true;
            }
        } else if (Histogram.MIN_DOC_COUNT_FIELD.match(currentFieldName)) {
            if (token == XContentParser.Token.VALUE_NUMBER) {
                otherOptions.put(Histogram.MIN_DOC_COUNT_FIELD, parser.longValue());
                return true;
            }
        } else if (Histogram.EXTENDED_BOUNDS_FIELD.match(currentFieldName)) {
            // FIXME: We can't reuse ExtendedBounds.PARSER here, so we just ignore it
        } else if (Histogram.ORDER_FIELD.match(currentFieldName)) {
            // FIXME: We can't resuse InternalOrder.Parser.parseOrderParam here, so we just ignore it
        } else if (START_FIELD.match(currentFieldName)) {
            if (token == XContentParser.Token.VALUE_NUMBER) {
                otherOptions.put(START_FIELD, parser.longValue());
                return true;
            }
        } else if (END_FIELD.match(currentFieldName)) {
            if (token == XContentParser.Token.VALUE_NUMBER) {
                otherOptions.put(END_FIELD, parser.longValue());
                return true;
            }
        }
        return false;
    }

    @Override
    protected ProportionalSumAggregationBuilder createFactory(String aggregationName, ValuesSourceType valuesSourceType, ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        final ProportionalSumAggregationBuilder builder = new ProportionalSumAggregationBuilder(aggregationName);
        Object interval = otherOptions.get(INTERVAL_FIELD);
        if (interval != null) {
            if (interval instanceof Long) {
                builder.interval((long) interval);
            } else {
                builder.dateHistogramInterval((DateHistogramInterval) interval);
            }
        }
        Object offset = otherOptions.get(Histogram.OFFSET_FIELD);
        if (offset != null) {
            builder.offset((long) offset);
        }
        Object keyed = otherOptions.get(Histogram.KEYED_FIELD);
        if (keyed != null) {
            builder.keyed((boolean) keyed);
        }
        Object minDocCount = otherOptions.get(Histogram.MIN_DOC_COUNT_FIELD);
        if (minDocCount != null) {
            builder.minDocCount((long) minDocCount);
        }
        Object start = otherOptions.get(START_FIELD);
        if (start != null) {
            builder.start((long)start);
        }
        Object end = otherOptions.get(END_FIELD);
        if (end != null) {
            builder.end((long)end);
        }
        return builder;
    }

}
