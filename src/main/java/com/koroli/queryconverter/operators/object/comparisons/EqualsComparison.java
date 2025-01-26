package com.koroli.queryconverter.operators.object.comparisons;

import com.koroli.queryconverter.operators.object.ObjectComparison;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * Implements an equality comparison for MongoDB queries.
 */
public class EqualsComparison implements ObjectComparison {

    /**
     * Compares the value using the `$eq` MongoDB operator.
     *
     * @param value the value to compare.
     * @return the MongoDB document for equality comparison.
     */
    @Override
    public Document compare(Object value) {
        return new Document("$eq", new ObjectId(value.toString()));
    }
}
