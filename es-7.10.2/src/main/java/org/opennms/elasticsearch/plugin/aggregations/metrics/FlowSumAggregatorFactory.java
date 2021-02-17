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

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Based on org.elasticsearch.search.aggregations.metrics.SumAggregatorFactory (from ES 7.11.0).
 */
class FlowSumAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final ValuesSourceConfig flowStart, flowEnd, sampling;

    FlowSumAggregatorFactory(String name,
                             ValuesSourceConfig config,
                             ValuesSourceConfig flowStart,
                             ValuesSourceConfig flowEnd,
                             ValuesSourceConfig sampling,
                             QueryShardContext context,
                             AggregatorFactory parent,
                             AggregatorFactories.Builder subFactoriesBuilder,
                             Map<String, Object> metadata) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);

        this.flowStart = flowStart;
        this.flowEnd = flowEnd;
        this.sampling = sampling;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new FlowSumAggregator(name, config, flowStart, flowEnd, sampling, searchContext, parent, metadata);
    }


    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return createUnmapped(searchContext, parent, metadata);
    }
}
