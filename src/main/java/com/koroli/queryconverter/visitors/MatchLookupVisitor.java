package com.koroli.queryconverter.visitors;

import com.koroli.queryconverter.utils.ExpressionUtils;
import com.koroli.queryconverter.utils.ValidationUtils;
import lombok.RequiredArgsConstructor;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;

/**
 * Generates a lookup subpipeline match step from the ON clause. Combines with
 * the WHERE part of the joined collection for optimization.
 */
// almost unchanged code from the "original" code for traversing the nodes...
@RequiredArgsConstructor
public class MatchLookupVisitor extends ExpressionVisitorAdapter {
    private final String joinAliasTable;
    private final String baseAliasTable;

    /**
     * Processes a column to adapt its name for the match lookup step.
     *
     * @param column the column to process
     */
    @Override
    public void visit(final Column column) {
        if (!ValidationUtils.isColumnExpression(column)) {
            return;
        }

        String columnName;
        if (column.getTable() != null) {
            columnName = ExpressionUtils.extractColumnName(column);
        } else {
            columnName = column.getColumnName();
        }

        if (!ExpressionUtils.isAliasOfColumn(column, joinAliasTable)) {
            column.setColumnName(formatColumnForLet(column, columnName));
            column.setTable(null);
        } else {
            column.setTable(null);
            column.setColumnName("$" + columnName);
        }
    }

    private String formatColumnForLet(Column column, String columnName) {
        if (column.getTable() == null || ExpressionUtils.isAliasOfColumn(column, baseAliasTable)) {
            return "$$" + columnName.replace(".", "_").toLowerCase();
        } else {
            return "$$" + column.getName(false).replace(".", "_").toLowerCase();
        }
    }
}