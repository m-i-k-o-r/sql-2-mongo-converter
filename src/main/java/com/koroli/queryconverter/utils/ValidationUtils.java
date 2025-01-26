package com.koroli.queryconverter.utils;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import lombok.experimental.UtilityClass;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;

/**
 * Utility class for query validation operations
 */
@UtilityClass
public class ValidationUtils {

    /**
     * Validates that the given condition is {@code true}. Throws a {@link QueryConversionException} if invalid
     *
     * @param condition    the boolean condition to check
     * @param errorMessage the exception message if the condition is {@code false}
     * @throws QueryConversionException if the condition is {@code false}
     */
    public static void validateTrue(
            boolean condition,
            String errorMessage
    ) throws QueryConversionException {

        if (!condition) {
            throw new QueryConversionException(errorMessage);
        }
    }

    /**
     * Validates that the given condition is {@code false}. Throws a {@link QueryConversionException} if invalid
     *
     * @param condition    the boolean condition to check
     * @param errorMessage the exception message if the condition is {@code true}
     * @throws QueryConversionException if the condition is {@code true}
     */
    public static void validateFalse(
            boolean condition,
            String errorMessage
    ) throws QueryConversionException {

        if (condition) {
            throw new QueryConversionException(errorMessage);
        }
    }

    /**
     * Determines if the given expression represents a column
     *
     * @param expression the {@link Expression} to evaluate
     * @return {@code true} if the expression is a column, otherwise {@code false}
     */
    public static boolean isColumnExpression(Expression expression) {
        return expression instanceof Column column
                && !column.getName(false).matches("^(\".*\"|true|false)$");
    }

    /**
     * Checks if the provided select items represent a "SELECT *" query
     *
     * @param selectItems list of {@link SelectItem}s
     * @return {@code true} if the query is "SELECT *", otherwise {@code false}
     */
    public static boolean isSelectAllQuery(List<SelectItem<?>> selectItems) {
        return selectItems != null
                && selectItems.size() == 1
                && selectItems.getFirst().getExpression() instanceof AllColumns;
    }

    /**
     * Checks if the provided select items perform a "COUNT(*)" query
     *
     * @param selectItems list of {@link SelectItem}s
     * @return {@code true} if the query is "COUNT(*)", otherwise {@code false}
     */
    public static boolean isCountAllQuery(List<SelectItem<?>> selectItems) {
        if (selectItems != null && selectItems.size() == 1) {
            Expression expression = selectItems.getFirst().getExpression();
            if (expression instanceof Function function) {
                return "count(*)".equalsIgnoreCase(function.toString());
            }
        }
        return false;
    }

    /**
     * Checks if any of the provided select items involve aggregation functions (e.g., MAX(), SUM())
     *
     * @param selectItems list of {@link SelectItem}s
     * @return {@code true} if any select item includes an aggregation function, otherwise {@code false}
     */
    public static boolean isTotalGroup(List<SelectItem<?>> selectItems) {
        return selectItems != null
                && selectItems.stream().anyMatch(item -> ExpressionUtils.isAggregateExpression(item.toString()));
    }
}
