package com.koroli.queryconverter.processors;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.FromInfo;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.query.MongoQueryHolder;
import com.koroli.queryconverter.utils.ProcessorUtils;
import com.koroli.queryconverter.visitors.AliasCleanerVisitor;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.LinkedList;
import java.util.List;

/**
 * Processes SQL GROUP BY clauses and converts them into MongoDB aggregation stages.
 */
public class GroupByProcessor implements QueryProcessor {

    /**
     * Main method to process GROUP BY fields and SELECT items, and populate the MongoDB query holder.
     *
     * @param sqlCommandInfo the SQL command info containing the group by fields and select items.
     * @param queryHolder    the MongoDB query holder to populate with aggregation stages.
     * @throws QueryConversionException if an error occurs during processing.
     */
    @Override
    public void process(
            SQLCommandInfo sqlCommandInfo,
            MongoQueryHolder queryHolder
    ) throws QueryConversionException {

        if (sqlCommandInfo.getGroupByFields().isEmpty()) {
            return;
        }

        List<String> groupByFields = preprocessGroupBy(sqlCommandInfo.getGroupByFields(), sqlCommandInfo.getFrom());
        List<SelectItem<?>> selects = preprocessSelect(sqlCommandInfo.getSelectItems(), sqlCommandInfo.getFrom());

        queryHolder.getAggregationWrapper().setGroupByFields(groupByFields);

        queryHolder.getProjectionWrapper().setProjection(
                ProcessorUtils.createProjectionsFromSelectItems(selects, groupByFields));

        queryHolder.getProjectionWrapper().setAliasProjection(
                ProcessorUtils.createAliasProjection(selects, groupByFields));

        queryHolder.getAggregationWrapper().setRequiresMultistepAggregation(true);
    }

    /**
     * Preprocesses the group by fields to remove table aliases.
     *
     * @param groupByFields the original group by fields.
     * @param fromInfo      the FROM clause information.
     * @return a list of group by fields without table aliases.
     */
    private List<String> preprocessGroupBy(List<String> groupByFields, FromInfo fromInfo) {
        List<String> processedGroupBy = new LinkedList<>();
        for (String field : groupByFields) {
            int index = field.indexOf(fromInfo.getBaseAlias() + ".");

            processedGroupBy.add(index != -1
                    ? field.substring(fromInfo.getBaseAlias().length() + 1)
                    : field);
        }
        return processedGroupBy;
    }

    /**
     * Preprocesses the select items to clean up aliases.
     *
     * @param selectItems the original select items.
     * @param fromInfo    the FROM clause information.
     * @return a list of processed select items.
     */
    private List<SelectItem<?>> preprocessSelect(List<SelectItem<?>> selectItems, FromInfo fromInfo) {
        for (SelectItem<?> selectItem : selectItems) {
            selectItem.accept(
                    new AliasCleanerVisitor(fromInfo.getBaseAlias()),
                    fromInfo
            );
        }
        return selectItems;
    }
}
