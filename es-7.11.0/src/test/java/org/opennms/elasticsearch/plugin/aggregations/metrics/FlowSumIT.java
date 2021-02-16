/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2021 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2021 The OpenNMS Group, Inc.
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

package org.opennms.elasticsearch.plugin.aggregations.metrics;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;
import org.opennms.elasticsearch.plugin.aggregations.DriftPlugin;
import org.opennms.elasticsearch.plugin.aggregations.bucket.histogram.FlowHistogramAggregationBuilder;

@ESIntegTestCase.SuiteScopeTestCase
public class FlowSumIT extends ESIntegTestCase {

    private static final String IDX = "idx";
    private static final String TYPE = "mytype";

    protected int numberOfShards() {
        return 2;
    }

    protected int numberOfReplicas() {
        return 1;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DriftPlugin.class);
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {

        byte[] template = copyToBytesFromClasspath("/range.template.json");

        client().admin().indices()
                .preparePutTemplate("range_template")
                .setSource(template, XContentType.JSON).get();

        createIndex(IDX);
        List<IndexRequestBuilder> builders = Stream
                .of(doc(2, 5, 4), doc(6, 12, 70), doc(10, 15, 600))
                .map(d -> indexDoc(d))
                .collect(Collectors.toList());
        indexRandom(true, builders);
        ensureSearchable();
    }

    private IndexRequestBuilder indexDoc(XContentBuilder builder) {
        return client().prepareIndex(IDX, TYPE).setSource(builder);
    }

    private XContentBuilder doc(long gte, long lte, long value) throws Exception {
        return jsonBuilder()
                .startObject()
                .startObject("range")
                .field("gte", gte)
                .field("lte", lte)
                .endObject()
                .field("start", gte)
                .field("end", lte)
                .field("value", value)
                .endObject();
    }

    @Test
    public void test() {
        SearchResponse response = client().prepareSearch(IDX)
                .setSize(0)
                .addAggregation(
                        new FlowHistogramAggregationBuilder("histo")
                        .interval(5)
                        .field("range")
                        .subAggregation(
                                new FlowSumAggregationBuilder("sum")
                                .field("value")
                                .start("start")
                                .end("end")
                        )
                )
                .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");

        System.out.println("histo: " + histo);
    }

}
