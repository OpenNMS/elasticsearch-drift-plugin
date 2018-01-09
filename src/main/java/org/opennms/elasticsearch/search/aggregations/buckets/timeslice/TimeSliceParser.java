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

import static org.elasticsearch.search.aggregations.bucket.histogram.Histogram.INTERVAL_FIELD;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

/**
 * see {@link org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsParser}
 */
public class TimeSliceParser extends MultiValuesSourceParser.NumericValuesSourceParser {

    public static final ParseField MULTIVALUE_MODE_FIELD = new ParseField("mode");
    public static final ParseField START_FIELD = new ParseField("start");
    public static final ParseField END_FIELD = new ParseField("end");

    public TimeSliceParser() {
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

        } else if (Histogram.KEYED_FIELD.match(currentFieldName)) {

        } else if (Histogram.MIN_DOC_COUNT_FIELD.match(currentFieldName)) {

        } else if (Histogram.EXTENDED_BOUNDS_FIELD.match(currentFieldName)) {

        } else if (Histogram.ORDER_FIELD.match(currentFieldName)) {

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
    protected TimeSliceAggregationBuilder createFactory(String aggregationName, ValuesSourceType valuesSourceType, ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        final TimeSliceAggregationBuilder builder = new TimeSliceAggregationBuilder(aggregationName);
        Object interval = otherOptions.get(INTERVAL_FIELD);
        if (interval != null) {
            if (interval instanceof Long) {
                builder.interval((long) interval);
            } else {
                builder.dateHistogramInterval((DateHistogramInterval) interval);
            }
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
