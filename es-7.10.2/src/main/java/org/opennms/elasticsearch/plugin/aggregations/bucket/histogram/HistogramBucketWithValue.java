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
package org.opennms.elasticsearch.plugin.aggregations.bucket.histogram;

import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;

/**
 * A histogram bucket that has some value.
 *
 * @author jwhite
 */
public interface HistogramBucketWithValue extends Histogram.Bucket {

    double getValue();

}