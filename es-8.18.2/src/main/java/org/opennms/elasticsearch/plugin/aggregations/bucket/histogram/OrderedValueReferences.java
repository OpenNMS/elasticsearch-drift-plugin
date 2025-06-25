/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2025 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2025 The OpenNMS Group, Inc.
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

import java.io.IOException;
import java.util.Optional;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

public class OrderedValueReferences {
    private final SortedNumericDoubleValues[] valuesArray;
    private final SortedNumericDoubleValues rangeStarts;
    private final SortedNumericDoubleValues rangeEnds;
    private final SortedNumericDoubleValues values;
    private final SortedNumericDoubleValues samplings;


    public OrderedValueReferences(LeafReaderContext ctx, MultiValuesSource.NumericMultiValuesSource valuesSources, String[] orderedFieldNames) throws IOException {
        // We can no longer count on the multi-valued fields being sorted in the order they
        // were passed in the query, so we re-order them based on the order in which they
        // were given in the query

        int rangeStartIndex = -1;
        int rangeEndIndex = -1;
        int valueIndex = -1;
        int samplingIndex = -1;

        final String fieldNames[] = valuesSources.fieldNames();

        if (fieldNames.length < 3 || fieldNames.length > 4) {
            throw new IllegalStateException("Invalid number of fields specified. Need 3 or 4, got " + fieldNames.length);
        }

        if (orderedFieldNames.length != fieldNames.length) {
            throw new IllegalStateException("Ordered list of field names does not have the same number of fields! This shouldn't happen.");
        }

        for (int i = 0; i < fieldNames.length; i++) {
            if (orderedFieldNames[0].equals(fieldNames[i])) {
                rangeStartIndex = i;
            } else if (orderedFieldNames[1].equals(fieldNames[i])) {
                rangeEndIndex = i;
            } else if (orderedFieldNames[2].equals(fieldNames[i])) {
                valueIndex = i;
            } else if (orderedFieldNames.length > 3 && orderedFieldNames[3].equals(fieldNames[i])) {
                samplingIndex = i;
            }
        }

        valuesArray = new SortedNumericDoubleValues[fieldNames.length];

        if (rangeStartIndex >= 0 && rangeEndIndex >= 0 && valueIndex >= 0 && (fieldNames.length == 3 || samplingIndex >= 0)) {
            // Use the known ordering
            valuesArray[0] = valuesSources.getField(fieldNames[rangeStartIndex], ctx);
            valuesArray[1] = valuesSources.getField(fieldNames[rangeEndIndex], ctx);
            valuesArray[2] = valuesSources.getField(fieldNames[valueIndex], ctx);
            if (fieldNames.length == 4) {
                valuesArray[3] = valuesSources.getField(fieldNames[samplingIndex], ctx);
            }
        } else {
            // Use the given ordering
            for (int i = 0; i < valuesArray.length; ++i) {
                valuesArray[i] = valuesSources.getField(fieldNames[i], ctx);
            }
        }

        rangeStarts = valuesArray[0];
        rangeEnds = valuesArray[1];
        values = valuesArray[2];
        if (valuesArray.length > 3) {
            samplings = valuesArray[3];
        } else {
            samplings = null;
        }
    }

    public SortedNumericDoubleValues[] getValuesArray() {
        return valuesArray;
    }

    public SortedNumericDoubleValues getRangeStarts() {
        return rangeStarts;
    }

    public SortedNumericDoubleValues getRangeEnds() {
        return rangeEnds;
    }

    public SortedNumericDoubleValues getValues() {
        return values;
    }

    public Optional<SortedNumericDoubleValues> getSamplings() {
        return Optional.ofNullable(samplings);
    }

}
