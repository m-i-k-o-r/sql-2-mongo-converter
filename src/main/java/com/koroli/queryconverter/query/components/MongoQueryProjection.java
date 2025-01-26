package com.koroli.queryconverter.query.components;

import lombok.Getter;
import lombok.Setter;
import org.bson.Document;

/**
 * Stores projection-related query components for MongoDB.
 * Includes fields to project and alias mappings.
 */
@Getter
@Setter
public class MongoQueryProjection {
    private Document projection = new Document();
    private Document aliasProjection = new Document();
}
