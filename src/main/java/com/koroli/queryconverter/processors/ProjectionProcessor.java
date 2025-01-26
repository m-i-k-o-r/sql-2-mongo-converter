package com.koroli.queryconverter.processors;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.query.MongoQueryHolder;
import com.koroli.queryconverter.utils.NormalizationUtils;
import com.koroli.queryconverter.utils.ParsingUtils;
import com.koroli.queryconverter.utils.ValidationUtils;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Processes SQL SELECT clauses and generates MongoDB projections.
 */
public class ProjectionProcessor implements QueryProcessor {

    /**
     * Processes SELECT items to create MongoDB projections.
     *
     * @param sqlCommandInfo the SQL command info containing select items.
     * @param queryHolder    the MongoDB query holder to populate with projections.
     * @throws QueryConversionException if an unsupported projection is encountered.
     */
    @Override
    public void process(
            SQLCommandInfo sqlCommandInfo,
            MongoQueryHolder queryHolder
    ) throws QueryConversionException {

        if (sqlCommandInfo.isDistinct()) {
            queryHolder.getProjectionWrapper().setProjection(
                    new Document(
                            sqlCommandInfo.getSelectItems().getFirst().toString(),
                            1
                    ));
            queryHolder.setDistinct(true);
        } else if (sqlCommandInfo.getGroupByFields().isEmpty()
                && !sqlCommandInfo.isTotalGroup()
                && !ValidationUtils.isSelectAllQuery(sqlCommandInfo.getSelectItems())
        ) {
            Document projections = new Document();
            projections.put("_id", 0);

            for (SelectItem<?> selectItem : sqlCommandInfo.getSelectItems()) {
                processSelectItem(selectItem, projections, sqlCommandInfo);
            }

            queryHolder.getProjectionWrapper().setProjection(projections);
        }
    }

    /**
     * Processes a single SELECT item and adds its projection to the given document.
     *
     * @param selectItem     the select item to process.
     * @param projections    the MongoDB projections document to update.
     * @param sqlCommandInfo the SQL command info containing field mappings.
     * @throws QueryConversionException if an unsupported expression is encountered.
     */
    private void processSelectItem(
            SelectItem<?> selectItem,
            Document projections,
            SQLCommandInfo sqlCommandInfo
    ) throws QueryConversionException {

        Expression expression = selectItem.getExpression();
        Alias alias = selectItem.getAlias();
        String key = alias != null
                ? alias.getName()
                : ParsingUtils.extractStringValue(expression);

        if (expression instanceof Column) {
            projections.put(key, 1);
        } else if (expression instanceof Function function) {
            Document functionProjection = new Document();
            parseFunction(function, functionProjection, sqlCommandInfo);
            projections.put(key, functionProjection);
        } else {
            throw new QueryConversionException("Unsupported project expression: " + expression);
        }
    }

    /**
     * Parses a SQL function and adds its MongoDB equivalent to the projections document.
     *
     * @param function           the SQL function to parse.
     * @param functionProjection the MongoDB function projection document to update.
     * @param sqlCommandInfo     the SQL command info containing field mappings.
     * @throws QueryConversionException if the function cannot be parsed.
     */
    private void parseFunction(
            Function function,
            Document functionProjection,
            SQLCommandInfo sqlCommandInfo
    ) throws QueryConversionException {

        String mongoFunctionName = ParsingUtils.translateFunctionName(function.getName());
        Object functionParams = parseFunctionParameters(function.getParameters(), sqlCommandInfo);

        functionProjection.put("$" + mongoFunctionName, functionParams);
    }

    /**
     * Parses function parameters into MongoDB-compatible format.
     *
     * @param parameters     the function parameters to parse.
     * @param sqlCommandInfo the SQL command info containing field mappings.
     * @return a parsed representation of the parameters.
     * @throws QueryConversionException if parameter parsing fails.
     */
    private Object parseFunctionParameters(
            ExpressionList<?> parameters,
            SQLCommandInfo sqlCommandInfo

    ) throws QueryConversionException {

        if (parameters == null) return null;

        List<Object> parsedParameters = new ArrayList<>();

        for (Expression parameter : parameters.getExpressions()) {
            Object normalizedValue = NormalizationUtils.normalizeExpression(
                    parameter,
                    null,
                    sqlCommandInfo.getFrom().getDefaultFieldType(),
                    sqlCommandInfo.getFrom().getFieldNameToFieldTypeMapping(),
                    null,
                    null);

            if (normalizedValue == null) {
                continue;
            }

            if (parameter instanceof Column) {
                normalizedValue = "$" + normalizedValue;
            }

            parsedParameters.add(normalizedValue);
        }

        return parsedParameters.size() == 1
                ? parsedParameters.getFirst()
                : parsedParameters;
    }
}
