package com.koroli.queryconverter.processors;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.query.MongoQueryHolder;
import net.sf.jsqlparser.statement.select.Select;
import org.bson.Document;

import java.util.LinkedList;
import java.util.List;

/**
 * Processor responsible for handling SQL subqueries in the FROM clause and converting them into
 * equivalent MongoDB aggregation pipelines.
 */
public class FromSubQueryProcessor implements QueryProcessor {

    /**
     * Processes a SQL subquery in the FROM clause and updates the MongoDB query holder
     * with the necessary aggregation steps.
     *
     * @param sqlCommandInfo the SQL command information.
     * @param queryHolder    the holder for MongoDB query components.
     * @throws QueryConversionException if an error occurs during processing.
     */
    @Override
    public void process(
            SQLCommandInfo sqlCommandInfo,
            MongoQueryHolder queryHolder
    ) throws QueryConversionException {

        if (sqlCommandInfo.getFrom().getBaseFrom() instanceof Select) {
            queryHolder.getAggregationWrapper().setPrevSteps(
                    generateAggregationSteps(
                            (SQLCommandInfo) sqlCommandInfo.getFrom().getBaseSQLHolder(),
                            queryHolder
                    ));
            queryHolder.getAggregationWrapper().setRequiresMultistepAggregation(true);
        }
    }

    /**
     * Generates a list of MongoDB aggregation steps based on the SQL command information.
     *
     * @param sqlCommandInfo the SQL command information.
     * @param queryHolder    the holder for MongoDB query components.
     * @return a list of MongoDB aggregation steps as {@link Document}.
     */
    private List<Document> generateAggregationSteps(
            SQLCommandInfo sqlCommandInfo,
            MongoQueryHolder queryHolder
    ) {
        List<Document> documents = initPipeline(queryHolder);

        // 1. Add $match stage for filters
        if (queryHolder.getFilterWrapper().getQuery() != null && !queryHolder.getFilterWrapper().getQuery().isEmpty()) {
            documents.add(new Document("$match", queryHolder.getFilterWrapper().getQuery()));
        }

        // 2. Add join stages if any
        if (sqlCommandInfo.getJoins() != null && !sqlCommandInfo.getJoins().isEmpty()) {
            documents.addAll(queryHolder.getAggregationWrapper().getJoinPipeline());
        }

        // 3. Add $group stage for aggregation
        if (!sqlCommandInfo.getGroupByFields().isEmpty() || sqlCommandInfo.isTotalGroup()) {
            documents.add(new Document("$group", prepareGroupStage(queryHolder)));
        }

        // 4. Add $match stage for HAVING clause
        if (queryHolder.getAggregationWrapper().getHaving() != null && !queryHolder.getAggregationWrapper().getHaving().isEmpty()) {
            documents.add(new Document("$match", queryHolder.getAggregationWrapper().getHaving()));
        }

        // 5. Add $sort stage for sorting
        if (queryHolder.getFilterWrapper().getSort() != null && !queryHolder.getFilterWrapper().getSort().isEmpty()) {
            documents.add(new Document("$sort", queryHolder.getFilterWrapper().getSort()));
        }

        // 6. Add $skip and $limit stages for pagination
        if (queryHolder.getFilterWrapper().getOffset() != -1) {
            documents.add(new Document("$skip", queryHolder.getFilterWrapper().getOffset()));
        }
        if (queryHolder.getFilterWrapper().getLimit() != -1) {
            documents.add(new Document("$limit", queryHolder.getFilterWrapper().getLimit()));
        }

        // 7. Add $project stage for alias projection
        Document aliasProjection = queryHolder.getProjectionWrapper().getAliasProjection();
        if (!aliasProjection.isEmpty()) {
            documents.add(new Document("$project", aliasProjection));
        }

        // 8. Handle projections without grouping
        if (sqlCommandInfo.getGroupByFields().isEmpty() && !sqlCommandInfo.isTotalGroup() && !queryHolder.getProjectionWrapper().getProjection().isEmpty()
        ) {
            documents.add(new Document("$project", queryHolder.getProjectionWrapper().getProjection()));
        }

        return documents;
    }

    /**
     * Initializes the aggregation pipeline, ensuring it starts with any pre-existing steps.
     *
     * @param queryHolder the holder for MongoDB query components.
     * @return a list of initial pipeline steps.
     */
    private List<Document> initPipeline(final MongoQueryHolder queryHolder) {
        List<Document> documents = queryHolder.getAggregationWrapper().getPrevSteps();
        if (documents == null || documents.isEmpty()) {
            documents = new LinkedList<>();
        }
        return documents;
    }

    /**
     * Prepares the $group stage for the MongoDB aggregation pipeline.
     *
     * @param queryHolder the holder for MongoDB query components.
     * @return a {@link Document} representing the $group stage.
     */
    private Document prepareGroupStage(MongoQueryHolder queryHolder) {
        Document projection = queryHolder.getProjectionWrapper().getProjection();

        if (!projection.containsKey("_id")) {
            Document groupStage = new Document("_id", new Document());

            projection.forEach((key, value) -> {
                if (!"_id".equals(key)) {
                    groupStage.put(key, value);
                }
            });

            return groupStage;
        }

        return projection;
    }
}
