package com.koroli.queryconverter.query.components;

import lombok.Getter;
import lombok.Setter;
import org.bson.Document;

/**
 * Stores filter-related query components for MongoDB.
 * Includes query filters, sorting, limit, and offset.
 */
@Getter
@Setter
public class MongoQueryFilters {
    private Document query = new Document();
    private Document sort = new Document();
    private long limit = -1;
    private long offset = -1;
}