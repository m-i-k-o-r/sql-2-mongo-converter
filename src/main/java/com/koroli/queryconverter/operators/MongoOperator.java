package com.koroli.queryconverter.operators;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import org.bson.Document;

/**
 * Interface representing a MongoDB operator for query conversion.
 * Provides methods to retrieve the column associated with the operator
 * and convert the operator into a MongoDB-compatible {@link Document}.
 */
public interface MongoOperator {

    /**
     * Retrieves the name of the target column for this operator.
     *
     * @return the column name as a {@link String}.
     */
    String getColumn();

    /**
     * Converts the operator into a MongoDB-compatible {@link Document}.
     *
     * @return the MongoDB document representing this operator.
     * @throws QueryConversionException if conversion to a document fails.
     */
    Document toDocument() throws QueryConversionException;
}