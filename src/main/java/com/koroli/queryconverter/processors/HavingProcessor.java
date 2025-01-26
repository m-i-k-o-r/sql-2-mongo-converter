package com.koroli.queryconverter.processors;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.AliasHolder;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.model.FieldType;
import com.koroli.queryconverter.query.MongoQueryHolder;
import com.koroli.queryconverter.utils.ExpressionUtils;
import com.koroli.queryconverter.utils.FunctionUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.bson.Document;

import java.util.Map;

/**
 * Processes SQL HAVING clauses and converts them into MongoDB aggregation filters.
 */
public class HavingProcessor extends WhereProcessor {

    /**
     * Constructor.
     *
     * @param defaultFieldType            the default field type.
     * @param fieldNameToFieldTypeMapping mapping of field names to their types.
     * @param requiresAggregation         whether aggregation is required for the SQL statement.
     * @param aliasHolder                 alias holder for field resolution.
     */
    public HavingProcessor(
            FieldType defaultFieldType,
            Map<String, FieldType> fieldNameToFieldTypeMapping,
            boolean requiresAggregation,
            AliasHolder aliasHolder
    ) {
        super(defaultFieldType, fieldNameToFieldTypeMapping, requiresAggregation, aliasHolder);
    }

    @Override
    public void process(
            SQLCommandInfo sqlCommandInfo,
            MongoQueryHolder queryHolder
    ) throws QueryConversionException {

        Expression havingClause = sqlCommandInfo.getHavingClause();

        if (havingClause != null) {
            Document parsedQuery = (Document) parseExpression(new Document(), havingClause, null);
            queryHolder.getAggregationWrapper().setHaving(parsedQuery);
        }
    }

    /**
     * Recursively processes functions within the SQL HAVING clause to generate MongoDB aggregation filters.
     *
     * @param query                       the MongoDB query document being constructed.
     * @param object                      the SQL function or expression to process.
     * @param defaultFieldType            the default field type.
     * @param fieldNameToFieldTypeMapping mapping of field names to their types.
     * @return the MongoDB filter equivalent of the SQL function or expression.
     * @throws QueryConversionException if processing fails.
     */
    @Override
    protected Object processFunctionExpressions(
            Document query,
            Object object,
            FieldType defaultFieldType,
            Map<String, FieldType> fieldNameToFieldTypeMapping
    ) throws QueryConversionException {

        if (object instanceof Function function) {
            String strFunction = function.toString();

            if (ExpressionUtils.isAggregateExpression(strFunction)) {
                String alias = aliasHolder.getAliasFromFieldExp(function.toString());
                return "$" + FunctionUtils.generateAggregationField(function, alias).getValue();
            }
        }
        return super.processFunctionExpressions(
                query,
                object,
                defaultFieldType,
                fieldNameToFieldTypeMapping
        );
    }

}

