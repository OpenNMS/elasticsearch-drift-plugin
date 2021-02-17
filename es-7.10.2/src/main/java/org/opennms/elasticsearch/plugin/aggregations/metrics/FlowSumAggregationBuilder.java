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

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.MetricAggregatorSupplier;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

/**
 * Based on org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder (from ES 7.11.0)
 */
public class FlowSumAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource.Numeric, FlowSumAggregationBuilder> {

    public static final String NAME = "flow_sum";

    public static final ValuesSourceRegistry.RegistryKey<MetricAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
            NAME,
            MetricAggregatorSupplier.class
    );

    public static final ParseField START_FIELD = new ParseField("start");
    public static final ParseField END_FIELD = new ParseField("end");
    public static final ParseField SAMPLING_FIELD = new ParseField("sampling");

    public static final ObjectParser<FlowSumAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME, FlowSumAggregationBuilder::new);

    static {
        // adds the "field", "script", and "missing" properties
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        // add properties for configuring the field names that determine the start, end, and sampling of flows
        PARSER.declareString(FlowSumAggregationBuilder::start, START_FIELD);
        PARSER.declareString(FlowSumAggregationBuilder::end, END_FIELD);
        PARSER.declareString(FlowSumAggregationBuilder::sampling, SAMPLING_FIELD);
        // start and end are required
        PARSER.declareRequiredFieldSet(START_FIELD.getPreferredName());
        PARSER.declareRequiredFieldSet(END_FIELD.getPreferredName());
    }

    // the name of the fields that specify the start and end of flows (inclusive) and the sampling factor
    private String start, end, sampling;

    public FlowSumAggregationBuilder(String name) {
        super(name);
    }

    public FlowSumAggregationBuilder(FlowSumAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.start = clone.start;
        this.end = clone.end;
        this.sampling = clone.sampling;
    }

    public FlowSumAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        start = in.readOptionalString();
        end = in.readOptionalString();
        sampling = in.readOptionalString();
    }

    public FlowSumAggregationBuilder start(String start) {
        this.start = start;
        return this;
    }

    public FlowSumAggregationBuilder end(String end) {
        this.end = end;
        return this;
    }

    public FlowSumAggregationBuilder sampling(String sampling) {
        this.sampling = sampling;
        return this;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(start);
        out.writeOptionalString(end);
        out.writeOptionalString(sampling);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }

    private ValuesSourceConfig valuesSourceConfig(QueryShardContext context, String field) {
        return ValuesSourceConfig.resolveUnregistered(context, null, field, null, null, null, null, CoreValuesSourceType.NUMERIC);
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
            QueryShardContext context,
            ValuesSourceConfig config,
            AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new FlowSumAggregatorFactory(
                name,
                config,
                valuesSourceConfig(context, START_FIELD.getPreferredName()),
                valuesSourceConfig(context, END_FIELD.getPreferredName()),
                valuesSourceConfig(context, SAMPLING_FIELD.getPreferredName()),
                context,
                parent,
                subFactoriesBuilder,
                metadata
        );
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new FlowSumAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
