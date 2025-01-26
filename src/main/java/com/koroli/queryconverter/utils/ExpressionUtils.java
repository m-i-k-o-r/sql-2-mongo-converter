package com.koroli.queryconverter.utils;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.AliasHolder;
import com.koroli.queryconverter.model.FieldType;
import lombok.experimental.UtilityClass;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.Arrays;
import java.util.Set;

/**
 * Utility class for handling and manipulating SQL expressions.
 */
@UtilityClass
public class ExpressionUtils {

    private static final Set<String> AGGREGATE_FUNCTIONS = Set.of("sum(", "avg(", "min(", "max(", "count(");

    /**
     * Converts a non-function expression to a node representation, prepending "$" if it is a column.
     *
     * @param expression          the expression to process.
     * @param requiresAggregation whether the expression requires aggregation.
     * @return the node representation of the expression.
     * @throws QueryConversionException if the conversion fails.
     */
    public static Object convertToNode(
            Expression expression,
            boolean requiresAggregation
    ) throws QueryConversionException {

        return (ValidationUtils.isColumnExpression(expression)
                && !expression.toString().startsWith("$")
                && requiresAggregation)
                        ? ("$" + expression)
                        : NormalizationUtils.normalizeExpression(expression, null, FieldType.UNKNOWN, null, new AliasHolder(), null);
    }

    /**
     * Checks if the given string represents an aggregate expression, such as MAX(), SUM(), etc.
     *
     * @param field the field to check.
     * @return {@code true} if the field is an aggregate expression, otherwise {@code false}.
     */
    public static boolean isAggregateExpression(String field) {
        if (field == null || field.isBlank()) {
            return false;
        }

        String fieldForAgg = field.trim().toLowerCase();
        return AGGREGATE_FUNCTIONS.stream().anyMatch(fieldForAgg::startsWith);
    }

    /**
     * Removes the table alias from a column name, leaving only the column name.
     *
     * @param column    the column to process.
     * @param aliasBase the alias to remove.
     * @return the updated column without the table alias.
     */
    public static Column stripAliasFromColumn(Column column, String aliasBase) {
        String columnName = column.getName(false);
        if (columnName.startsWith(aliasBase + ".")) {
            column.setColumnName(columnName.substring(aliasBase.length() + 1));
        }
        column.setTable(null);
        return column;
    }

    /**
     * Removes the table alias from a {@link SelectItem}, leaving only the column name.
     *
     * @param selectItem the {@link SelectItem} to process.
     * @param aliasBase  the alias to remove.
     * @return the updated {@link SelectItem} without the table alias.
     */
    public static SelectItem<?> stripAliasFromSelectItem(
            SelectItem<?> selectItem,
            String aliasBase
    ) {
        if (selectItem != null && selectItem.getExpression() instanceof Column column) {
            stripAliasFromColumn(column, aliasBase);
        }
        return selectItem;
    }

    /**
     * Extracts the column name from a nested field, removing the table alias or prefix.
     *
     * @param column the column to process.
     * @return the extracted column name without prefixes.
     */
    public static String extractColumnName(Column column) {
        String[] fieldParts = column.getName(false).split("\\.");
        return (fieldParts.length > 2)
                    ? String.join(".", Arrays.copyOfRange(fieldParts, 1, fieldParts.length))
                    : fieldParts[fieldParts.length - 1];
    }

    /**
     * Checks if a given table alias is part of the column name.
     *
     * @param column     the column to check.
     * @param tableAlias the table alias to verify.
     * @return {@code true} if the alias is part of the column name, otherwise {@code false}.
     */
    public static boolean isAliasOfColumn(Column column, String tableAlias) {
        return column.getName(false).startsWith(tableAlias);
    }

    /**
     * Updates the type of {@link Join} to "INNER JOIN" if it starts with "join".
     *
     * @param join the {@link Join} to update.
     */
    public static void setInnerJoinIfDefault(Join join) {
        if (join.toString().toLowerCase().startsWith("join ")) {
            join.setInner(true);
        }
    }
}
