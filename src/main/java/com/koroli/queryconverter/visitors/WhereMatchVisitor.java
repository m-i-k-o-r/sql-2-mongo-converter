package com.koroli.queryconverter.visitors;

import com.koroli.queryconverter.utils.ExpressionUtils;
import com.koroli.queryconverter.utils.ValidationUtils;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Generates a lookup match step from WHERE clauses. Combines with the "ON" part of joined collections for optimization.
 */
// almost unchanged code from the "original" code for traversing the nodes...
@RequiredArgsConstructor
public class WhereMatchVisitor extends ExpressionVisitorAdapter {

    private final String baseAliasTable;
    @Setter
    private Optional<Expression> expressionOptional;
    private final AtomicBoolean haveOrExpression;

    private boolean isBaseAliasOrValue;

    public WhereMatchVisitor(String baseAliasTable, Optional<Expression> optionalExpression, AtomicBoolean haveOrExpression) {
        this.baseAliasTable = baseAliasTable;
        this.expressionOptional = optionalExpression;
        this.haveOrExpression = haveOrExpression;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(Column column) {
        if (ValidationUtils.isColumnExpression(column)) {
            isBaseAliasOrValue = ExpressionUtils.isAliasOfColumn(column, baseAliasTable);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(OrExpression expr) {
        haveOrExpression.set(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(IsNullExpression expr) {
        if (isBaseAliasOrValue) {
            setExpressionOptional(Optional.of(setOrAndExpression(expressionOptional.get(), expr)));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(final InExpression expr) {
        isBaseAliasOrValue = true;
        expr.getLeftExpression().accept(this);

        if (!isBaseAliasOrValue) {
            expr.getRightExpression().accept(this);
        } else {
            if (expr.getRightExpression() instanceof ExpressionList<?> expressionList) {
                for (Expression item : expressionList.getExpressions()) {
                    item.accept(this);
                }
            } else {
                expr.getRightExpression().accept(this);
            }

            if (isBaseAliasOrValue) {
                setExpressionOptional(Optional.of(setOrAndExpression(expressionOptional.get(), expr)));
            }
        }
    }

    private Expression setOrAndExpression(Expression baseExpression, Expression newExpression) {
        return baseExpression != null
                ? new AndExpression(baseExpression, newExpression)
                : newExpression;
    }
}
