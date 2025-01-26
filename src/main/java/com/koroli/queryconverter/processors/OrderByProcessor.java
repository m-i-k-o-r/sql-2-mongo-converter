package com.koroli.queryconverter.processors;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.AliasHolder;
import com.koroli.queryconverter.holders.FromInfo;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.query.MongoQueryHolder;
import com.koroli.queryconverter.utils.ExpressionUtils;
import com.koroli.queryconverter.utils.ParsingUtils;
import com.koroli.queryconverter.utils.ProcessorUtils;
import com.koroli.queryconverter.visitors.AliasCleanerVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.bson.Document;

import java.util.List;

public class OrderByProcessor implements QueryProcessor {
    @Override
    public void process(
            SQLCommandInfo sqlCommandInfo,
            MongoQueryHolder queryHolder
    ) throws QueryConversionException {

        if (sqlCommandInfo.getOrderByElements() != null && !sqlCommandInfo.getOrderByElements().isEmpty()) {
            queryHolder.getFilterWrapper().setSort(
                    buildSortInfo(
                            prepareOrderByElements(sqlCommandInfo.getOrderByElements(), sqlCommandInfo.getFrom()),
                            sqlCommandInfo.getAliasHolder(),
                            sqlCommandInfo.getGroupByFields()
                    )
            );
        }
    }

    /**
     * Generates the MongoDB sort information from the provided SQL `ORDER BY` elements.
     *
     * @param orderByElements the list of `OrderByElement` to process.
     * @param aliasHolder     the holder for aliases used in the SQL query.
     * @param groupByFields   the list of fields used in the `GROUP BY` clause.
     * @return a {@link Document} representing the sort information for MongoDB.
     * @throws QueryConversionException if an error occurs during the conversion process.
     */
    private Document buildSortInfo(
            List<OrderByElement> orderByElements,
            AliasHolder aliasHolder,
            List<String> groupByFields
    ) throws QueryConversionException {
        Document document = new Document();

        for (OrderByElement orderByElement : orderByElements) {
            String sortKey;

            if (orderByElement.getExpression() instanceof Function function) {
                String alias = aliasHolder.getAliasFromFieldExp(function.toString());
                if (alias != null && !alias.equals(function.toString())) {
                    sortKey = alias;
                } else {
                    Document parseFunctionDocument = new Document();
                    ProcessorUtils.processAggregationFunction(function, parseFunctionDocument, null);
                    sortKey = parseFunctionDocument.keySet().iterator().next();
                }
            } else {
                String sortField = ParsingUtils.extractStringValue(orderByElement.getExpression());
                sortKey = aliasHolder.getFieldFromAliasOrField(sortField);

                if (!groupByFields.isEmpty()) {
                    if (!ExpressionUtils.isAggregateExpression(sortKey)) {
                        sortKey = groupByFields.size() > 1
                                ? "_id." + sortKey.replaceAll("\\.", "_")
                                : "_id";
                    }
                }
            }

            document.put(sortKey, orderByElement.isAsc() ? 1 : -1);
        }

        return document;
    }

    /**
     * Prepares the list of {@link OrderByElement} by cleaning up any aliases and ensuring they are ready for processing.
     *
     * @param orderByElements list to preprocess.
     * @param fromInfo        containing details about the base alias.
     * @return the updated list.
     */
    private List<OrderByElement> prepareOrderByElements(List<OrderByElement> orderByElements, FromInfo fromInfo) {
        for (OrderByElement item : orderByElements) {
            item.getExpression().accept(
                    new AliasCleanerVisitor(fromInfo.getBaseAlias())
            );
        }
        return orderByElements;
    }
}
