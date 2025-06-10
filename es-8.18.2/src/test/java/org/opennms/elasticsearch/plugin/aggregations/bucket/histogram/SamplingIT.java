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

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.test.ESIntegTestCase;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.Test;
import org.opennms.elasticsearch.plugin.DriftPlugin;

@ESIntegTestCase.SuiteScopeTestCase
public class SamplingIT extends ESIntegTestCase {

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
                indexDoc(1, 1, 1, 31, 100, 1.0),
                indexDoc(1, 1, 1, 31, 1, 100.0),
                indexDoc(1, 1, 1, 31, 2, 50.0),
                indexDoc(1, 1, 1, 31, 3, 100.0 / 3.0)));
        indexRandom(true, builders);
        ensureSearchable();
    }

    private Instant date(int month, int day) {
        return Instant.parse(String.format(Locale.ROOT, "2012-%02d-%02dT00:00:00Z", month, day));
    }

    private Instant date(String date) {
        return Instant.parse(date);
    }

    private IndexRequestBuilder indexDoc(int month, int day, int value, double interval) throws Exception {
        return indexDoc(month, day, month + 1, day + 1, value, interval);
    }

    private IndexRequestBuilder indexDoc(int startMonth, int startDay, int endMonth, int endDay, int value, double interval) throws Exception {
        final Instant start = date(startMonth, startDay);
        final Instant end = date(endMonth, endDay);

        return client().prepareIndex("idx").setSource(jsonBuilder()
                .startObject()
                .field("value", value)
                .field("constant", 1)
                .field("start", start)
                .field("end", end)
                .field("interval", interval)
                .endObject());
    }

    @Test
    public void testAggregateWithInterval() {
        SearchResponse response = client().prepareSearch("idx")
                .setSize(0)
                .addAggregation(new ProportionalSumAggregationBuilder("histo")
                        .fields(Arrays.asList("start", "end", "value", "interval"))
                        .dateHistogramInterval(DateHistogramInterval.MONTH)
                        .start(Instant.parse("2012-01-01T00:00:00Z").toEpochMilli())
                        .end(Instant.parse("2012-01-31T00:00:00Z").toEpochMilli())
                        .order(BucketOrder.key(true))
                )
                .execute().actionGet();
        
        try {

            Histogram histo = response.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends HistogramBucketWithValue> buckets = (List<? extends HistogramBucketWithValue>) histo.getBuckets();
            assertThat(buckets.size(), equalTo(1));

            assertThat(buckets.get(0).getKey(), equalTo(Instant.parse("2012-01-01T00:00:00Z")));
            assertThat(buckets.get(0).getDocCount(), equalTo(4L));
            assertThat(buckets.get(0).getValue(), closeTo(400.0, 0.01));
        } finally {
            response.decRef();
        }
    }

    @Test
    public void testAggregateWithoutInterval() {
        SearchResponse response = client().prepareSearch("idx")
                .setSize(0)
                .addAggregation(new ProportionalSumAggregationBuilder("histo")
                        .fields(Arrays.asList("start", "end", "value"))
                        .dateHistogramInterval(DateHistogramInterval.MONTH)
                        .start(Instant.parse("2012-01-01T00:00:00Z").toEpochMilli())
                        .end(Instant.parse("2012-01-31T00:00:00Z").toEpochMilli())
                        .order(BucketOrder.key(true))
                )
                .execute().actionGet();
        
        try {

            Histogram histo = response.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends HistogramBucketWithValue> buckets = (List<? extends HistogramBucketWithValue>) histo.getBuckets();
            assertThat(buckets.size(), equalTo(1));

            assertThat(buckets.get(0).getKey(), equalTo(Instant.parse("2012-01-01T00:00:00Z")));
            assertThat(buckets.get(0).getDocCount(), equalTo(4L));
            assertThat(buckets.get(0).getValue(), closeTo(106.0, 0.01));
        } finally {
            response.decRef();
        }
    }
}
