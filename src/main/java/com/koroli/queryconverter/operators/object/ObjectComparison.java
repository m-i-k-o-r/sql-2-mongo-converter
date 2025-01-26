package com.koroli.queryconverter.operators.object;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import org.bson.Document;

/**
 * Interface for defining comparison strategies for objects in MongoDB.
 */
public interface ObjectComparison {

    /**
     * Compares the given value and returns a MongoDB-compatible {@link Document}.
     *
     * @param value the value to compare.
     * @return the MongoDB document representing the comparison.
     * @throws QueryConversionException if the comparison fails.
     */
    Document compare(Object value) throws QueryConversionException;
}