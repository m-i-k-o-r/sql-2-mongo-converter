package com.koroli.queryconverter.query.components;

import lombok.Getter;
import lombok.Setter;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Stores update-related query components for MongoDB.
 * Includes fields to update and unset.
 */
@Getter
@Setter
public class MongoQueryUpdate {
    private Document updateSet = new Document();
    private List<String> fieldsToUnset = new ArrayList<>();
}
