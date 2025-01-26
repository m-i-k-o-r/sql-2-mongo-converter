package com.koroli.queryconverter.query.components;

import lombok.Getter;
import lombok.Setter;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Stores aggregation-related query components for MongoDB.
 * Includes group-by fields, having conditions, and pipeline steps.
 */
@Getter
@Setter
public class MongoQueryAggregation {
    private List<String> groupByFields = new ArrayList<>();
    private Document having = new Document();
    private List<Document> joinPipeline = new ArrayList<>();
    private List<Document> prevSteps = new ArrayList<>();
    private boolean requiresMultistepAggregation = false;
}
