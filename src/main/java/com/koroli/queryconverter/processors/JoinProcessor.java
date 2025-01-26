package com.koroli.queryconverter.processors;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.AliasHolder;
import com.koroli.queryconverter.holders.FromInfo;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.model.FieldType;
import com.koroli.queryconverter.query.MongoQueryHolder;
import com.koroli.queryconverter.visitors.AliasCleanerVisitor;
import com.koroli.queryconverter.visitors.MatchLookupVisitor;
import com.koroli.queryconverter.visitors.OnClauseVisitor;
import com.koroli.queryconverter.visitors.WhereMatchVisitor;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Processes SQL JOIN clauses and converts them into MongoDB aggregation stages.
 */
public final class JoinProcessor implements QueryProcessor {

    /**
     * Processes SQL JOIN clauses, converting them into MongoDB aggregation steps.
     *
     * @param sqlCommandInfo the SQL command info containing join details.
     * @param queryHolder    the MongoDB query holder to populate with join pipelines.
     * @throws QueryConversionException if an error occurs during processing.
     */
    @Override
    public void process(
            SQLCommandInfo sqlCommandInfo,
            MongoQueryHolder queryHolder
    ) throws QueryConversionException {

        List<Join> joins = sqlCommandInfo.getJoins();
        if (joins == null || joins.isEmpty()) {
            return;
        }

        queryHolder.getAggregationWrapper().setRequiresMultistepAggregation(true);
        try {
            List<Document> joinPipeline = createJoinPipeline(sqlCommandInfo);
            queryHolder.getAggregationWrapper().setJoinPipeline(joinPipeline);
        } catch (Exception e) {
            throw new QueryConversionException("Error processing JOIN clause", e);
        }
    }

    /**
     * Creates a pipeline of MongoDB aggregation steps for processing JOIN clauses.
     *
     * @param sqlCommandInfo the SQL command info containing join and where details.
     * @return a list of MongoDB aggregation documents representing the join pipeline.
     * @throws QueryConversionException if an error occurs during processing.
     */
    private List<Document> createJoinPipeline(SQLCommandInfo sqlCommandInfo) throws QueryConversionException {
        List<Document> aggregationPipeline = new LinkedList<>();
        FromInfo fromInfo = sqlCommandInfo.getFrom();
        Expression whereCondition = sqlCommandInfo.getWhereClause();
        AtomicBoolean haveOrExpression = new AtomicBoolean(false);

        if (whereCondition != null) {
            detectOrExpression(whereCondition, haveOrExpression);
        }

        for (Join join : sqlCommandInfo.getJoins()) {
            if (!join.isInner() && !join.isLeft()) {
                throw new QueryConversionException("Only INNER and LEFT JOINs are supported");
            }

            if (!(join.getRightItem() instanceof Table rightTable)) {
                throw new QueryConversionException("Currently only table-based JOIN is supported");
            }

            String rightTableName = rightTable.getName();
            String rightTableAlias = rightTable.getAlias() != null
                    ? rightTable.getAlias().getName()
                    : rightTableName;

            Expression onExp = join.getOnExpression();

            if (whereCondition != null) {
                haveOrExpression.set(false);
                whereCondition.accept(
                        new WhereMatchVisitor(
                                rightTableAlias,
                                haveOrExpression
                        )
                );
                if (!haveOrExpression.get() && onExp != null) {
                    onExp.accept(
                            new AliasCleanerVisitor(
                                    rightTableAlias
                            )
                    );
                } else {
                    onExp = null;
                }
            }

            Expression extraWhereExp = !haveOrExpression.get() && whereCondition != null ?
                    combineExpressions(onExp, whereCondition)
                    : null;

            aggregationPipeline.add(
                    buildLookupStep(
                            fromInfo,
                            rightTableName,
                            rightTableAlias,
                            onExp,
                            extraWhereExp
                    ));

            aggregationPipeline.add(
                    buildUnwindStep(
                            rightTableAlias,
                            join.isLeft()
                    ));
        }

        if (haveOrExpression.get() && whereCondition != null) {
            aggregationPipeline.add(buildInternalMatchAfterJoin(fromInfo, whereCondition));
        }

        return aggregationPipeline;
    }

    /**
     * Combines two SQL expressions using an AND operator.
     *
     * @param left  the left expression.
     * @param right the right expression.
     * @return the combined expression.
     */
    private Expression combineExpressions(Expression left, Expression right) {
        if (left == null) return right;
        if (right == null) return left;
        return new AndExpression(left, right);
    }

    /**
     * Builds a $lookup aggregation step for MongoDB.
     *
     * @param fromInfo       the left table information.
     * @param joinCollection the right table name.
     * @param joinAlias      the alias for the joined table.
     * @param onExp          the ON condition of the JOIN.
     * @param extraWhereExp  additional WHERE conditions to include.
     * @return a MongoDB $lookup aggregation step.
     * @throws QueryConversionException if an error occurs during processing.
     */
    private Document buildLookupStep(
            FromInfo fromInfo,
            String joinCollection,
            String joinAlias,
            Expression onExp,
            Expression extraWhereExp
    ) throws QueryConversionException {

        List<Document> aggregationPipeline = new ArrayList<>();

        if (onExp != null || extraWhereExp != null) {
            Expression combinedExp = combineExpressions(onExp, extraWhereExp);
            aggregationPipeline.add(buildLookupMatchStep(fromInfo, combinedExp, joinAlias));
        }

        Document letVariables = new Document();
        onExp.accept(
                new OnClauseVisitor(letVariables, joinAlias, fromInfo.getBaseAlias())
        );

        return new Document(
                "$lookup",
                new Document()
                        .append("from", joinCollection)
                        .append("let", letVariables)
                        .append("pipeline", aggregationPipeline)
                        .append("as", joinAlias)
        );
    }

    /**
     * Builds $unwind aggregation step for MongoDB.
     *
     * @param rightTableAlias the alias of the joined table.
     * @param isLeftJoin      whether the JOIN is a LEFT JOIN.
     * @return a MongoDB $unwind aggregation step.
     */
    private Document buildUnwindStep(String rightTableAlias, boolean isLeftJoin) {
        return new Document(
                "$unwind",
                new Document()
                        .append("path", "$" + rightTableAlias)
                        .append("preserveNullAndEmptyArrays", isLeftJoin)
        );
    }

    /**
     * Builds a $match step for use within a $lookup or as a final pipeline step.
     *
     * @param fromInfo   the left table information.
     * @param expression the condition to match.
     * @return a MongoDB $match aggregation step.
     * @throws QueryConversionException if the condition cannot be parsed.
     */
    private Document buildLookupMatchStep(
            FromInfo fromInfo,
            Expression expression,
            String rightTableAlias
    ) throws QueryConversionException {
        Document match = new Document();

        expression.accept(new MatchLookupVisitor(rightTableAlias, fromInfo.getBaseAlias()));

        WhereProcessor whereProcessor = new WhereProcessor(
                fromInfo.getDefaultFieldType(),
                fromInfo.getFieldNameToFieldTypeMapping(),
                false,
                new AliasHolder()
        );
        Document parsed = (Document) whereProcessor.parseExpression(new Document(), expression, null);
        match.put("$match", parsed);
        return match;
    }

    private static Document buildInternalMatchAfterJoin(
            FromInfo fromInfo,
            Expression whereExpression
    ) throws QueryConversionException {
        WhereProcessor whereProcessor = new WhereProcessor(
                FieldType.UNKNOWN,
                Collections.emptyMap(),
                false,
                new AliasHolder()
        );

        whereExpression.accept(new AliasCleanerVisitor(fromInfo.getBaseAlias()));

        Document match = new Document();
        match.put("$match", whereProcessor.parseExpression(new Document(), whereExpression, null));
        return match;
    }

    /**
     * Detects whether a given expression contains OR conditions.
     *
     * @param expression       the expression to analyze.
     * @param haveOrExpression a flag that will be set to true if an OR condition is detected.
     */
    private void detectOrExpression(Expression expression, AtomicBoolean haveOrExpression) {
        if (expression instanceof OrExpression) {
            haveOrExpression.set(true);
        } else if (expression instanceof BinaryExpression binaryExpression) {
            detectOrExpression(binaryExpression.getLeftExpression(), haveOrExpression);
            detectOrExpression(binaryExpression.getRightExpression(), haveOrExpression);
        }
    }
}