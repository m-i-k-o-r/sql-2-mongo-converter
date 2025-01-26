package com.koroli.queryconverter.holders;

/**
 * Interface for classes that hold query-related information.
 * Provides a method to retrieve the name of the base table in a query.
 */
public interface Holder {

    /**
     * Retrieves the name of the base table from the query.
     *
     * @return the base table name as a {@link String}.
     */
    String getBaseTableName();

}