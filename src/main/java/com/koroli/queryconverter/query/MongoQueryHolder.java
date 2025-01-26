package com.koroli.queryconverter.query;

import com.koroli.queryconverter.model.SQLCommandType;
import com.koroli.queryconverter.query.components.MongoQueryAggregation;
import com.koroli.queryconverter.query.components.MongoQueryFilters;
import com.koroli.queryconverter.query.components.MongoQueryProjection;
import com.koroli.queryconverter.query.components.MongoQueryUpdate;
import lombok.Getter;
import lombok.Setter;

/**
 * Holds all query components for building a MongoDB query.
 */
@Getter
@Setter
public class MongoQueryHolder {
    private final String collection;
    private final SQLCommandType sqlCommandType;

    /**
     * Filter-related components
     */
    private final MongoQueryFilters filterWrapper = new MongoQueryFilters();

    /**
     * Projection-related components
     */
    private final MongoQueryProjection projectionWrapper = new MongoQueryProjection();

    /**
     * Aggregation-related components
     */
    private final MongoQueryAggregation aggregationWrapper = new MongoQueryAggregation();

    /**
     * Update-related components
     */
    private final MongoQueryUpdate updateWrapper = new MongoQueryUpdate();

    private boolean distinct = false;
    private boolean countAll = false;

    /**
     * Constructs a new MongoDBQueryHolder.
     *
     * @param collection     the MongoDB collection to query.
     * @param sqlCommandType the type of SQL command (e.g., SELECT, DELETE).
     */
    public MongoQueryHolder(String collection, SQLCommandType sqlCommandType) {
        this.collection = collection;
        this.sqlCommandType = sqlCommandType;
    }
}