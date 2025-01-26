package com.koroli.queryconverter.operators.regex;

import com.koroli.queryconverter.operators.MongoOperator;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import org.bson.Document;

/**
 * Represents a MongoDB regex operator for query conversion.
 * This operator allows matching or negating strings based on a regular expression.
 */
@Value
@Builder
public class RegexOperator implements MongoOperator {
    String column;
    String regex;
    boolean isNot;

    @With
    String options;

    /**
     * Converts this regex operator into a MongoDB-compatible {@link Document}.
     *
     * @return the MongoDB document representation of this regex operator.
     */
    @Override
    public Document toDocument() {
        Document regexDoc = new Document("$regex", regex);
        if (options != null) {
            regexDoc.append("$options", options);
        }

        return new Document(
                column,
                isNot ? new Document("$not", regexDoc) : regexDoc
        );
    }
}
