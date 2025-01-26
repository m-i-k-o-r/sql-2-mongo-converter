package com.koroli.queryconverter.processors;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.query.MongoQueryHolder;

/**
 * Processes COUNT(*) queries and sets the corresponding flag in the MongoDB query holder.
 */
public class CountAllProcessor implements QueryProcessor {

    /**
     * Sets the count-all flag in the MongoDB query holder if the SQL command indicates COUNT(*).
     *
     * @param sqlCommandInfo the SQL command info containing query details.
     * @param queryHolder    the MongoDB query holder to update.
     * @throws QueryConversionException if an error occurs during processing.
     */
    @Override
    public void process(
            SQLCommandInfo sqlCommandInfo,
            MongoQueryHolder queryHolder
    ) throws QueryConversionException {

        if (sqlCommandInfo.isCountAll()) {
            queryHolder.setCountAll(true);
        }
    }
}
