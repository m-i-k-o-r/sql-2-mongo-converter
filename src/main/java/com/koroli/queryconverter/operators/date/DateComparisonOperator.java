package com.koroli.queryconverter.operators.date;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import lombok.experimental.UtilityClass;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;

import java.util.Map;

/**
 * Utility class for converting SQL comparison operators into MongoDB operators.
 */
@UtilityClass
public class DateComparisonOperator {

    /**
     * Mapping of SQL comparison operator class names to MongoDB operator strings.
     */
    private static final Map<String, String> OPERATORS = Map.of(
            "GreaterThanEquals", "$gte",
            "GreaterThan",       "$gt",
            "MinorThanEquals",   "$lte",
            "MinorThan",         "$lt",
            "EqualsTo",          "$eq"
    );

    /**
     * Converts a SQL comparison operator into its MongoDB equivalent.
     *
     * @param comparisonOperator the SQL comparison operator to convert.
     * @return the MongoDB operator string.
     * @throws QueryConversionException if the operator is unsupported or null.
     */
    public static String parseOperator(ComparisonOperator comparisonOperator) throws QueryConversionException {
        if (comparisonOperator == null) {
            throw new QueryConversionException("Comparison operator cannot be null");
        }

        String operator = OPERATORS.get(comparisonOperator.getClass().getSimpleName());

        if (operator == null) {
            throw new QueryConversionException("Unsupported comparison operator: " + comparisonOperator.getClass().getSimpleName());
        }
        return operator;
    }
}
