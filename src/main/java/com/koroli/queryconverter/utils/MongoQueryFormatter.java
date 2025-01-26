package com.koroli.queryconverter.utils;

import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.query.MongoQueryHolder;
import lombok.experimental.UtilityClass;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Utility class to format MongoDB queries based on SQL command information and query holders.
 */
@UtilityClass
public class MongoQueryFormatter {

    /**
     * Formats a MongoDB query based on the given query holder and SQL command information.
     *
     * @param queryHolder             The MongoDB query holder containing query details.
     * @param sqlCommandInfo          The SQL command information.
     * @param aggregationAllowDiskUse Whether disk usage is allowed for aggregation.
     * @param aggregationBatchSize    The batch size for aggregation.
     * @return A formatted MongoDB query string.
     */
    public static String formatQuery(
            MongoQueryHolder queryHolder,
            SQLCommandInfo sqlCommandInfo,
            Boolean aggregationAllowDiskUse,
            Integer aggregationBatchSize
    ) {
        StringBuilder formattedQuery = new StringBuilder();
        JsonWriterSettings settings = JsonWriterSettings.builder()
                .indent(true)
                .build();

        String collection = queryHolder.getCollection();
        Document query = queryHolder.getFilterWrapper().getQuery();
        Document projection = queryHolder.getProjectionWrapper().getProjection();
        Document sort = queryHolder.getFilterWrapper().getSort();
        Document updateSet = queryHolder.getUpdateWrapper().getUpdateSet();
        List<String> updateUnSet = queryHolder.getUpdateWrapper().getFieldsToUnset();

        // 1. distinct query
        if (queryHolder.isDistinct()) {
            formattedQuery
                    .append("db.").append(collection).append(".distinct(")
                    .append("\"").append(getDistinctFieldName(queryHolder)).append("\", ")
                    .append(query.toJson(settings)).append(")");
        }

        // 2. count query
        else if (queryHolder.isCountAll() && !isAggregate(queryHolder, sqlCommandInfo)) {
            formattedQuery
                    .append("db.").append(collection).append(".count(")
                    .append(query.toJson(settings)).append(")");
        }

        // 3. aggregate query
        else if (isAggregate(queryHolder, sqlCommandInfo)) {
            formattedQuery
                    .append("db.").append(collection).append(".aggregate(")
                    .append("[");

            List<Document> pipeline = getAggregationPipeline(queryHolder, sqlCommandInfo);
            for (int i = 0; i < pipeline.size(); i++) {
                formattedQuery.append(pipeline.get(i).toJson(settings));
                if (i < pipeline.size() - 1) {
                    formattedQuery.append(", ");
                }
            }
            formattedQuery.append("]");

            Document options = getAggregationOptions(aggregationAllowDiskUse, aggregationBatchSize);

            if (!options.isEmpty()) {
                formattedQuery.append(new Document("options", options).toJson(settings));
            }

            formattedQuery.append(")");
        }

        // 4. find, delete, and update queries
        else {
            String command = "find";
            if (queryHolder.getSqlCommandType() != null) {
                command = switch (queryHolder.getSqlCommandType()) {
                    case DELETE  -> "remove";
                    case UPDATE  -> "updateMany";
                    default      -> "find";
                };
            }

            formattedQuery.append("db.").append(collection).append(".").append(command).append("(")
                    .append(query.toJson(settings));

            if (projection != null && !projection.isEmpty() && "find".equals(command)) {
                formattedQuery.append(", ").append(projection.toJson(settings));
            }
            formattedQuery.append(")");

            if (sort != null && !sort.isEmpty() && "find".equals(command)) {
                formattedQuery.append(".sort(").append(sort.toJson(settings)).append(")");
            }

            if (queryHolder.getFilterWrapper().getOffset() > 0 && "find".equals(command)) {
                formattedQuery.append(".skip(").append(queryHolder.getFilterWrapper().getOffset()).append(")");
            }

            if (queryHolder.getFilterWrapper().getLimit() > 0 && "find".equals(command)) {
                formattedQuery.append(".limit(").append(queryHolder.getFilterWrapper().getLimit()).append(")");
            }

            if ("updateMany".equals(command)) {
                Document updateDocument = new Document();
                if (updateSet != null && !updateSet.isEmpty()) {
                    updateDocument.put("$set", updateSet);
                }
                if (updateUnSet != null && !updateUnSet.isEmpty()) {
                    updateDocument.put("$unset", updateUnSet);
                }
                formattedQuery.append(", ").append(updateDocument.toJson(settings)).append(")");
            }
        }

        return formattedQuery.toString();
    }

    /**
     * Constructs aggregation options for MongoDB queries.
     *
     * @param aggregationAllowDiskUse Whether disk usage is allowed for aggregation.
     * @param aggregationBatchSize    The batch size for aggregation.
     * @return A Document containing aggregation options.
     */
    private static Document getAggregationOptions(Boolean aggregationAllowDiskUse, Integer aggregationBatchSize) {
        Document options = new Document();
        if (aggregationAllowDiskUse != null) {
            options.put("allowDiskUse", aggregationAllowDiskUse);
        }

        if (aggregationBatchSize != null) {
            options.put("cursor", new Document("batchSize", aggregationBatchSize));
        }
        return options;
    }

    /**
     * Checks whether the query is an aggregate query.
     *
     * @param queryHolder    The MongoDB query holder.
     * @param sqlCommandInfo The SQL command information.
     * @return True if the query is an aggregate query, false otherwise.
     */
    private boolean isAggregate(MongoQueryHolder queryHolder, SQLCommandInfo sqlCommandInfo) {
        return sqlCommandInfo.getAliasHolder() != null
                && !sqlCommandInfo.getAliasHolder().isEmpty()
                || !sqlCommandInfo.getGroupByFields().isEmpty()
                || sqlCommandInfo.getJoins() != null
                && !sqlCommandInfo.getJoins().isEmpty()
                || queryHolder.getAggregationWrapper().getPrevSteps() != null
                && !queryHolder.getAggregationWrapper().getPrevSteps().isEmpty()
                || sqlCommandInfo.isTotalGroup()
                && !ValidationUtils.isCountAllQuery(sqlCommandInfo.getSelectItems());
    }

    /**
     * Builds the aggregation pipeline for the given query holder and SQL command information.
     *
     * @param queryHolder The MongoDB query holder.
     * @param sqlCommandInfo     The SQL command information.
     * @return A list of Documents representing the aggregation pipeline.
     */
    private List<Document> getAggregationPipeline(MongoQueryHolder queryHolder, SQLCommandInfo sqlCommandInfo) {
        List<Document> pipeline = new LinkedList<>();

        if (queryHolder.getAggregationWrapper().getPrevSteps() != null) {
            pipeline.addAll(queryHolder.getAggregationWrapper().getPrevSteps());
        }

        if (queryHolder.getFilterWrapper().getQuery() != null && !queryHolder.getFilterWrapper().getQuery().isEmpty()) {
            pipeline.add(new Document("$match", queryHolder.getFilterWrapper().getQuery()));
        }

        if (sqlCommandInfo.getJoins() != null && !sqlCommandInfo.getJoins().isEmpty()) {
            pipeline.addAll(queryHolder.getAggregationWrapper().getJoinPipeline());
        }

        Document aliasProjection;
        if (!sqlCommandInfo.getGroupByFields().isEmpty() || sqlCommandInfo.isTotalGroup()) {
            if (queryHolder.getProjectionWrapper().getProjection().get("_id") == null) {
                aliasProjection = new Document();
                aliasProjection.put("_id", new Document());

                for (Map.Entry<String, Object> keyValue : queryHolder.getProjectionWrapper().getProjection().entrySet()) {
                    if (!(keyValue.getKey()).equals("_id")) {
                        aliasProjection.put(keyValue.getKey(), keyValue.getValue());
                    }
                }

                pipeline.add(new Document("$group", aliasProjection));
            } else {
                pipeline.add(new Document("$group", queryHolder.getProjectionWrapper().getProjection()));
            }
        }

        if (queryHolder.getAggregationWrapper().getHaving() != null && !queryHolder.getAggregationWrapper().getHaving().isEmpty()) {
            pipeline.add(new Document("$match", queryHolder.getAggregationWrapper().getHaving()));
        }

        if (queryHolder.getFilterWrapper().getSort() != null && !queryHolder.getFilterWrapper().getSort().isEmpty()) {
            pipeline.add(new Document("$sort", queryHolder.getFilterWrapper().getSort()));
        }

        if (queryHolder.getFilterWrapper().getOffset() > 0) {
            pipeline.add(new Document("$skip", queryHolder.getFilterWrapper().getOffset()));
        }

        if (queryHolder.getFilterWrapper().getLimit() > 0) {
            pipeline.add(new Document("$limit", queryHolder.getFilterWrapper().getLimit()));
        }

        Document doc = queryHolder.getProjectionWrapper().getAliasProjection();
        if (!doc.isEmpty()) {
            pipeline.add(new Document("$project", doc));
        }

        if (sqlCommandInfo.getGroupByFields().isEmpty() && !sqlCommandInfo.isTotalGroup() && !queryHolder.getProjectionWrapper().getProjection().isEmpty()) {
            Document projection = queryHolder.getProjectionWrapper().getProjection();
            pipeline.add(new Document("$project", projection));
        }

        return pipeline;
    }

    /**
     * Gets the field name for a distinct query.
     *
     * @param queryHolder The MongoDB query holder.
     * @return The name of the field for the distinct query.
     */
    private String getDistinctFieldName(MongoQueryHolder queryHolder) {
        return queryHolder.getProjectionWrapper().getProjection().keySet().iterator().next();
    }
}

