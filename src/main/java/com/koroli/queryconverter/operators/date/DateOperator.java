package com.koroli.queryconverter.operators.date;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.operators.MongoOperator;
import lombok.Builder;
import lombok.Value;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import org.bson.Document;

import java.util.Date;

/**
 * Represents a MongoDB operator for handling date comparisons.
 */
@Value
public class DateOperator implements MongoOperator {
    Date date;
    String column;
    String comparisonExpression;

    /**
     * Constructs a {@link DateOperator} using the given format, value, column, and comparison operator.
     *
     * @param format the format of the date.
     * @param value the date value to parse.
     * @param column the target column for the operation.
     * @param operator the SQL comparison operator.
     * @throws QueryConversionException if date parsing or operator conversion fails.
     */
    @Builder
    public DateOperator(
            String format,
            String value,
            String column,
            ComparisonOperator operator
    ) throws QueryConversionException {
        this.date = DateParser.parse(format, value);
        this.column = column;
        this.comparisonExpression = DateComparisonOperator.parseOperator(operator);
    }

    /**
     * Converts this operator into a MongoDB-compatible {@link Document}.
     *
     * @return the MongoDB document representation of this operator.
     */
    @Override
    public Document toDocument() {
        return new Document(
                column,
                new Document(comparisonExpression, date)
        );
    }
}
