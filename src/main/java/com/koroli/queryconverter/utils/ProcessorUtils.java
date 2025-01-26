package com.koroli.queryconverter.utils;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import lombok.experimental.UtilityClass;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.bson.Document;

import java.util.List;
import java.util.Map;

@UtilityClass
public class ProcessorUtils {

    /**
     * Parses a SQL function and adds it to the MongoDB aggregation pipeline.
     *
     * @param function the SQL function to parse.
     * @param document the MongoDB document to update.
     * @param alias    the alias for the function.
     * @throws QueryConversionException if the function cannot be parsed.
     */
    public static void processAggregationFunction(
            Function function,
            Document document,
            Alias alias
    ) throws QueryConversionException {
        String operation = function.getName().toLowerCase();
        String aggField = FunctionUtils.generateAggregationField(function, alias).getValue();

        switch (operation) {
            case "count" ->
                    //document.put(aggField, new Document("$sum", 1));
                    applyAggregationFunction(
                            "sum",
                            aggField,
                            document,
                            1
                    );
            case "sum", "min", "max", "avg" ->
                    applyAggregationFunction(
                            operation,
                            aggField,
                            document,
                            "$" + ParsingUtils.extractFieldFromFunction(function)
                    );
            default -> throw new QueryConversionException("Unknown function: " + function.getName());
        }
    }

    private static void applyAggregationFunction(
            String functionName,
            String aggField,
            Document document,
            Object value
    ) {
        document.put(
                aggField,
                new Document("$" + functionName, value)
        );
    }

    /**
     * Creates MongoDB projections based on select items and group by fields.
     *
     * @param selectItems   the list of select items.
     * @param groupByFields the list of group by fields.
     * @return a MongoDB Document representing projections.
     * @throws QueryConversionException if an error occurs during processing.
     */
    public Document createProjectionsFromSelectItems(List<SelectItem<?>> selectItems, List<String> groupByFields) throws QueryConversionException {
        /*Document projections = new Document();

        for (SelectItem<?> selectItem : selectItems) {
            Expression expression = selectItem.getExpression();
            String alias = selectItem.getAlias() != null
                    ? selectItem.getAlias().getName()
                    : ParsingUtils.extractStringValue(expression);

            if (expression instanceof Function function) {
                ProcessorUtils.processAggregationFunction(function, projections, selectItem.getAlias());
            } else {
                projections.put(alias, "$" + alias);
            }
        }

        return projections;*/

        Document projections = new Document();

        List<SelectItem<?>> functionItems = selectItems.stream()
                .filter(selectItem -> selectItem.getExpression() instanceof Function)
                .toList();

        List<SelectItem<?>> nonFunctionItems = selectItems.stream()
                .filter(selectItem -> !functionItems.contains(selectItem))
                .toList();

        Document idDocument = new Document();

        for (SelectItem<?> selectItem : nonFunctionItems) {
            Column column = (Column) selectItem.getExpression();
            String columnName = ParsingUtils.extractStringValue(column);
            idDocument.put(columnName.replaceAll("\\.", "_"), "$" + columnName);
        }

        if (!idDocument.isEmpty()) {
            projections.append("_id", idDocument.size() == 1 ? idDocument.values().iterator().next() : idDocument);
        }

        for (SelectItem<?> selectItem : functionItems) {
            Function function = (Function) selectItem.getExpression();
            ProcessorUtils.processAggregationFunction(function, projections, selectItem.getAlias());
        }

        return projections;
    }

    /**
     * Creates alias projections for the provided select items and group by fields.
     *
     * @param selectItems   the list of select items.
     * @param groupByFields the list of group by fields.
     * @return a MongoDB Document representing alias projections.
     * @throws QueryConversionException if an error occurs during processing.
     */
    public Document createAliasProjection(List<SelectItem<?>> selectItems, List<String> groupByFields) throws QueryConversionException {
        Document aliasProjection = new Document();

        List<SelectItem<?>> functionItems = selectItems.stream()
                .filter(selectItem -> selectItem.getExpression() instanceof Function)
                .toList();

        List<SelectItem<?>> nonFunctionItems = selectItems.stream()
                .filter(selectItem -> !(selectItem.getExpression() instanceof Function))
                .toList();

        for (SelectItem<?> selectItem : nonFunctionItems) {
            Column column = (Column) selectItem.getExpression();
            String columnName = ParsingUtils.extractStringValue(column);
            Alias alias = selectItem.getAlias();
            String nameOrAlias = alias != null ? alias.getName() : columnName;

            aliasProjection.put(
                    nameOrAlias,
                    groupByFields.contains(columnName)
                            ? "$_id." + columnName.replaceAll("\\.", "_")
                            : "$" + columnName);
        }

        for (SelectItem<?> selectItem : functionItems) {
            Function function = (Function) selectItem.getExpression();
            Alias alias = selectItem.getAlias();
            Map.Entry<String, String> fieldToAliasMapping = FunctionUtils.generateAggregationField(function, alias);

            aliasProjection.put(fieldToAliasMapping.getValue(), 1);
        }

        aliasProjection.put("_id", 0);
        return aliasProjection;
    }
}
