package com.koroli.queryconverter.visitors;

import com.koroli.queryconverter.utils.ExpressionUtils;
import lombok.RequiredArgsConstructor;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import org.bson.Document;

/**
 * Visitor to generate lookup "lets" from the ON clause. Handles all fields without table fields.
 */
// almost unchanged code from the "original" code for traversing the nodes...
@RequiredArgsConstructor
public class OnClauseVisitor extends ExpressionVisitorAdapter {

    private final Document onDocument;
    private final String joinAliasTable;
    private final String baseAliasTable;

    /**
     * Processes a column to add it to the "let" document.
     *
     * @param column the column to process
     */
    @Override
    public void visit(final Column column) {
        String columnName;
        if (ExpressionUtils.isAliasOfColumn(column, joinAliasTable)) {
            return;
        }

        if (ExpressionUtils.isAliasOfColumn(column, baseAliasTable)) {
            columnName = ExpressionUtils.extractColumnName(column);
        } else {
            columnName = column.getName(false);
        }

        String formattedKey = columnName.replace(".", "_").toLowerCase();
        onDocument.put(formattedKey, "$" + columnName);
    }
}
