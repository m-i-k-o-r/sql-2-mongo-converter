package com.koroli.queryconverter.utils;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import lombok.experimental.UtilityClass;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing SQL expressions and handling related operations
 */
@UtilityClass
public class ParsingUtils {

    /**
     * Pattern to identify strings surrounded by double quotes
     */
    private static final Pattern QUOTED_STRING_PATTERN = Pattern.compile("^\"(.+)*\"$");

    /**
     * Mapping of function names to their translated equivalents
     */
    private static final Map<String, String> FUNCTION_NAME_MAPPING = Map.of(
            "OID",       "toObjectId",
            "TIMESTAMP", "toDate"
    );

    /**
     * Extracts the string value of an {@link Expression}, removing surrounding quotes if present.
     *
     * @param expression the {@link Expression} to process.
     * @return the extracted string value without quotes.
     */
    public static String extractStringValue(Expression expression) {
        if (expression instanceof StringValue stringValue) {
            return stringValue.getValue();
        } else if (expression instanceof Column column) {
            Matcher matcher = QUOTED_STRING_PATTERN.matcher(column.toString());
            return matcher.matches()
                    ? matcher.group(1)
                    : column.toString();
        }
        return expression.toString();
    }

    /**
     * Translates a function name to its equivalent custom name if available.
     *
     * @param functionName the original function name.
     * @return the translated function name, or the original if no translation is available.
     */
    public static String translateFunctionName(String functionName) {
        return FUNCTION_NAME_MAPPING.getOrDefault(functionName, functionName);
    }

    /**
     * Extracts the field name from a {@link Function}, e.g., MAX(advance_amount) -> advance_amount.
     *
     * @param function the {@link Function} to process.
     * @return the extracted field name, or {@code null} if invalid or absent.
     * @throws QueryConversionException if the function has multiple parameters.
     */
    public static String extractFieldFromFunction(Function function) throws QueryConversionException {
        if (function.getParameters() instanceof ExpressionList expressionList) {

            List<Expression> expressions = expressionList.getExpressions();
            if (expressions.size() == 1 && expressions.getFirst() instanceof AllColumns) {
                return null;
            }

            List<String> parameters = expressions.stream()
                    .map(ParsingUtils::extractStringValue)
                    .toList();

            if (parameters.size() > 1) {
                throw new QueryConversionException(function.getName() + " function can only have one parameter");
            }

            return !parameters.isEmpty()
                    ? parameters.getFirst()
                    : null;
        }
        return null;
    }
}
