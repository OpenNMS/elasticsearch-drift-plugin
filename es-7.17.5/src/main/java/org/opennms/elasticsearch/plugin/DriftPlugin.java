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
package org.opennms.elasticsearch.plugin;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.opennms.elasticsearch.plugin.aggregations.bucket.histogram.InternalProportionalSumHistogram;
import org.opennms.elasticsearch.plugin.aggregations.bucket.histogram.ProportionalSumAggregationBuilder;

public class DriftPlugin extends Plugin implements SearchPlugin {

    @Override
    public List<AggregationSpec> getAggregations() {
        return Arrays.asList(
                new AggregationSpec(ProportionalSumAggregationBuilder.NAME,
                        ProportionalSumAggregationBuilder::new,
                        ProportionalSumAggregationBuilder::parse).addResultReader(InternalProportionalSumHistogram::new)
        );
    }

}
