package com.koroli.queryconverter.processors;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.query.MongoQueryHolder;

/**
 * Interface for processing SQL queries.
 */
public interface QueryProcessor {
    /**
     * Processes a part of the SQL query and updates the MongoDBQueryHolder.
     *
     * @param sqlCommandInfo information about the SQL query
     * @param queryHolder    the object to store the MongoDB query data
     * @throws QueryConversionException if an error occurs during processing
     */
    void process(SQLCommandInfo sqlCommandInfo, MongoQueryHolder queryHolder) throws QueryConversionException;
}