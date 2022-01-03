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

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.test.ESIntegTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.opennms.elasticsearch.plugin.DriftPlugin;

@ESIntegTestCase.SuiteScopeTestCase
public class OffsetIT extends ESIntegTestCase {

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DriftPlugin.class);
    }

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(DriftPlugin.class);
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx", "idx_unmapped");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.addAll(Arrays.asList(
                indexDoc(11, 31, 1)
        ));
        indexRandom(true, builders);
        ensureSearchable();
    }

    private DateTime date(int hourOfDay, int minuteOfHour) {
        return new DateTime(2018, 2, 12, hourOfDay, minuteOfHour, DateTimeZone.UTC);
    }

    private DateTime date(String date) {
        return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseJoda(date);
    }

    private IndexRequestBuilder indexDoc(int hourOfDay, int minuteOfHour, int value) throws Exception {
        return indexDoc(hourOfDay, minuteOfHour, hourOfDay, minuteOfHour+1, value);
    }

    private IndexRequestBuilder indexDoc(int startHourOfDay, int startMinuteOfHour, int endHourOfDay, int endMinuteOfHour, int value) throws Exception {
        final DateTime start = date(startHourOfDay, startMinuteOfHour);
        final DateTime end = date(endHourOfDay, endMinuteOfHour);

        return client().prepareIndex("idx", "type").setSource(jsonBuilder()
                .startObject()
                .field("value", value)
                .field("constant", 1)
                .field("start", start)
                .field("end", end)
                .field("interval", 1.0)
                .endObject());
    }

    @Test
    public void testOffsetCalculation() {
        DateTime start = new DateTime(2018, 2, 12, 11, 10, DateTimeZone.UTC);
        DateTime end = new DateTime(2018, 2, 12, 11, 40, DateTimeZone.UTC);

        SearchResponse response = client().prepareSearch("idx")
                .setSize(0)
                .addAggregation(new ProportionalSumAggregationBuilder("histo")
                        .fields(Arrays.asList("start", "end", "value", "interval"))
                        .dateHistogramInterval(DateHistogramInterval.MONTH)
                        .start(start.getMillis())
                        .end(end.getMillis())
                        .interval(30 * 1000)
                        .order(BucketOrder.key(true))
                )
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends HistogramBucketWithValue> buckets = (List<? extends HistogramBucketWithValue>) histo.getBuckets();
        assertThat(buckets.size(), equalTo(1));

        assertThat(buckets.get(0).getDocCount(), equalTo(1L));
        // The bucket key should match the given start
        assertThat(buckets.get(0).getKey(), equalTo(start));
        assertThat(buckets.get(0).getValue(), closeTo(1.00d, 0.01));
    }

}
