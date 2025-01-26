package com.koroli.queryconverter.operators.object.comparisons;

import com.koroli.queryconverter.operators.object.ObjectComparison;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * Implements a not-equal comparison for MongoDB queries.
 */
public class NotEqualsComparison implements ObjectComparison {

    /**
     * Compares the value using the `$ne` MongoDB operator.
     *
     * @param value the value to compare.
     * @return the MongoDB document for not-equal comparison.
     */
    @Override
    public Document compare(Object value) {
        return new Document("$ne", new ObjectId(value.toString()));
    }
}
