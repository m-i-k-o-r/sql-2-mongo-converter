package com.koroli.queryconverter.operators.object.comparisons;

import com.koroli.queryconverter.operators.object.ObjectComparison;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * Implements an inclusion comparison (`$in` or `$nin`) for MongoDB queries.
 */
@RequiredArgsConstructor
public class InComparison implements ObjectComparison {
    private final boolean isNot;

    /**
     * Compares the values using the `$in` or `$nin` MongoDB operator.
     *
     * @param value the list of values to compare.
     * @return the MongoDB document for inclusion comparison.
     */
    @Override
    public Document compare(Object value) {
        List<String> stringList = (List<String>) value;
        List<ObjectId> objectIdList = stringList.stream()
                .map(ObjectId::new)
                .toList();
        return new Document(isNot ? "$nin" : "$in", objectIdList);
    }
}
