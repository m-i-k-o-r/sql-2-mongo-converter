package com.koroli.queryconverter.processors;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.FromInfo;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.query.MongoQueryHolder;
import com.koroli.queryconverter.utils.ProcessorUtils;
import com.koroli.queryconverter.visitors.AliasCleanerVisitor;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.Collections;
import java.util.List;

/**
 * Processes SQL Total Grouping clauses and converts them into MongoDB aggregation stages.
 */
public class TotalGroupProcessor implements QueryProcessor {

    /**
     * Processes total grouping, creating necessary projections and alias mappings.
     *
     * @param sqlCommandInfo the SQL command info containing grouping details.
     * @param queryHolder    the MongoDB query holder to populate with aggregation details.
     * @throws QueryConversionException if an error occurs during processing.
     */
    @Override
    public void process(
            SQLCommandInfo sqlCommandInfo,
            MongoQueryHolder queryHolder
    ) throws QueryConversionException {

        if (sqlCommandInfo.getGroupByFields().isEmpty() && sqlCommandInfo.isTotalGroup()) {
            List<SelectItem<?>> selects = preprocessSelect(sqlCommandInfo.getSelectItems(), sqlCommandInfo.getFrom());

            queryHolder.getProjectionWrapper().setProjection(
                    ProcessorUtils.createProjectionsFromSelectItems(selects, Collections.emptyList()));

            queryHolder.getProjectionWrapper().setAliasProjection(
                    ProcessorUtils.createAliasProjection(selects, Collections.emptyList()));
        }
    }

    /**
     * Preprocesses the select items to clean up aliases.
     *
     * @param selectItems the original select items.
     * @param fromInfo    the FROM clause information.
     * @return a list of processed select items.
     */
    private List<SelectItem<?>> preprocessSelect(final List<SelectItem<?>> selectItems, final FromInfo fromInfo) {
        for (SelectItem<?> selectItem : selectItems) {
            selectItem.accept(
                    new AliasCleanerVisitor(fromInfo.getBaseAlias()), fromInfo
            );
        }
        return selectItems;
    }
}
