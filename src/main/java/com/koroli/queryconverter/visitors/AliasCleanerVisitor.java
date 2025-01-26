package com.koroli.queryconverter.visitors;

import com.koroli.queryconverter.utils.ExpressionUtils;
import lombok.RequiredArgsConstructor;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * Visitor to remove table aliases from columns in SQL expressions.
 */
// almost unchanged code from the "original" code for traversing the nodes...
@RequiredArgsConstructor
public class AliasCleanerVisitor extends ExpressionVisitorAdapter {

    private final String baseAliasTable;

    /**
     * Removes the table alias from the column.
     *
     * @param column the column to process
     */
    @Override
    public void visit(final Column column) {
        ExpressionUtils.stripAliasFromColumn(column, baseAliasTable);
    }

    /**
     * Removes the table alias from the select expression item.
     *
     * @param selectExpressionItem the select expression item to process
     */
    @Override
    public void visit(final SelectItem selectExpressionItem) {
        ExpressionUtils.stripAliasFromSelectItem(selectExpressionItem, baseAliasTable);
    }

    /**
     * No operation needed for all columns; avoids StackOverflowException.
     *
     * @param allColumns the all columns object
     */
    @Override
    public void visit(final AllColumns allColumns) {
        // No operation needed
    }
}
