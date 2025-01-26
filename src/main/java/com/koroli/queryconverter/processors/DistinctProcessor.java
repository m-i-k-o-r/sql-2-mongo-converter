package com.koroli.queryconverter.processors;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.query.MongoQueryHolder;
import org.bson.Document;

/**
 * Processor responsible for handling DISTINCT keyword in SQL queries
 * and translating it into MongoDB projection for uniqueness.
 */
public class DistinctProcessor implements QueryProcessor {

    /**
     * Processes a DISTINCT query and updates the MongoDB query holder
     * to reflect the distinct projection.
     *
     * @param sqlCommandInfo the SQL command information.
     * @param queryHolder   the holder for MongoDB query components.
     * @throws QueryConversionException if an error occurs during processing.
     */
    @Override
    public void process(
            SQLCommandInfo sqlCommandInfo,
            MongoQueryHolder queryHolder
    ) throws QueryConversionException {

        if (sqlCommandInfo.isDistinct()) {
            Document projection = new Document();
            projection.put(sqlCommandInfo.getSelectItems().getFirst().toString(), 1);

            queryHolder.getProjectionWrapper().setProjection(projection);
            queryHolder.setDistinct(true);
        }
    }
}

