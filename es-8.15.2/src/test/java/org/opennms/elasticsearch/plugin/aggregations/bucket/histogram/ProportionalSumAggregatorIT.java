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

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.After;
import org.junit.Test;
import org.opennms.elasticsearch.plugin.DriftPlugin;

@ESIntegTestCase.SuiteScopeTestCase
public class ProportionalSumAggregatorIT extends ESIntegTestCase {

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
                indexDoc(1, 2, 1),  // start: Jan 2, end: Feb 3
                indexDoc(2, 2, 2),  // start: Feb 2, end: Mar 3
                indexDoc(2, 15, 3), // start: Feb 15, end: Mar 16
                indexDoc(3, 2, 4),  // start: Mar 2, end: Apr 3
                indexDoc(3, 15, 5), // start: Mar 15, end: Apr 16
                indexDoc(3, 23, 6), // start: Mar 23, end: Apr 24
                indexDoc(1,1, 4, 23, 6), // start: Jan 1, end: Apr 23
                indexDoc(1,2, 1, 2, 7))); // start: Jan 2, end: start
        indexRandom(true, builders);
        ensureSearchable();
    }

    private ZonedDateTime date(int month, int day) {
        return ZonedDateTime.of(2012, month, day, 0, 0, 0, 0, ZoneOffset.UTC);
    }

    private ZonedDateTime date(String date) {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
        return ZonedDateTime.parse(date, formatter);
    }

    private IndexRequestBuilder indexDoc(int month, int day, int value) throws Exception {
        return indexDoc(month, day, month+1, day+1, value);
    }
    private IndexRequestBuilder indexDoc(int startMonth, int startDay, int endMonth, int endDay, int value) throws Exception {
        final ZonedDateTime start = date(startMonth, startDay);
        final ZonedDateTime end = date(endMonth, endDay);

        return client().prepareIndex("idx").setSource(jsonBuilder()
                .startObject()
                .field("value", value)
                .field("constant", 1)
                .field("start", start)
                .field("end", end)
                .endObject());
    }



    @Test
    public void testAggregate() {
        ZonedDateTime start = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime end = ZonedDateTime.of(2012, 5, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        SearchResponse response = client().prepareSearch("idx")
                .setSize(0)
                .addAggregation(new ProportionalSumAggregationBuilder("histo")
                        .fields(Arrays.asList("start", "end", "value"))
                        .dateHistogramInterval(new DateHistogramInterval("1M"))// Approximate month interval
                        .timeZone(ZoneOffset.UTC)
                        .start(start.toInstant().toEpochMilli())
                        .end(end.toInstant().toEpochMilli())
                        .order(BucketOrder.key(true))
                )
                .get();
        try {
        ElasticsearchAssertions.assertNoFailures(response);
        ElasticsearchAssertions.assertAllSuccessful(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends HistogramBucketWithValue> buckets = (List<? extends HistogramBucketWithValue>) histo.getBuckets();
        buckets.stream().forEach(x-> x.getKey());
        System.out.println(  buckets);
        assertThat(buckets.size(), equalTo(4));

        double totalSumFromBuckets = 0;
        int i = 0;
        for (HistogramBucketWithValue bucket : buckets) {
            assertThat(bucket.getKey(), equalTo(ZonedDateTime.of(2012, i + 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)));
            totalSumFromBuckets += bucket.getValue();
            i++;
        }

        assertThat(totalSumFromBuckets, closeTo(34d, 0.01));

        assertThat(buckets.get(0).getDocCount(), equalTo(3L));
        assertThat(buckets.get(0).getValue(), closeTo(9.58d, 0.01));
        assertThat(buckets.get(1).getDocCount(), equalTo(4L));
        assertThat(buckets.get(1).getValue(), closeTo(4.97d, 0.01));
        assertThat(buckets.get(2).getDocCount(), equalTo(6L));
        assertThat(buckets.get(2).getValue(), closeTo(11.37d, 0.01));
        assertThat(buckets.get(3).getDocCount(), equalTo(4L));
        assertThat(buckets.get(3).getValue(), closeTo(8.07d, 0.01));
        } finally {
         // release the underlying buffer to avoid the Netty leak
            response.decRef();
        }
    }
}
