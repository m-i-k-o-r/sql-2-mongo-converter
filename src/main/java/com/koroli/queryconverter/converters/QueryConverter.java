package com.koroli.queryconverter.converters;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.AliasHolder;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.model.FieldType;
import com.koroli.queryconverter.processors.*;
import com.koroli.queryconverter.query.MongoQueryHolder;
import com.koroli.queryconverter.utils.MongoQueryFormatter;
import com.koroli.queryconverter.utils.ValidationUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Main class responsible for query conversion.
 */
@Getter
public final class QueryConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryConverter.class);

    private final Map<String, FieldType> fieldNameToFieldTypeMapping;
    private final FieldType defaultFieldType;

    private final Boolean aggregationAllowDiskUse;
    private final Integer aggregationBatchSize;

    private final List<QueryProcessor> processors;

    private final boolean logQueryEnabled;

    /**
     * Initializes a QueryConverter instance and processes the SQL input.
     */
    @Builder
    public QueryConverter(
            Map<String, FieldType> fieldNameToFieldTypeMapping,
            FieldType defaultFieldType,
            Boolean aggregationAllowDiskUse,
            Integer aggregationBatchSize,
            Boolean logQueryEnabled
    ) {
        this.defaultFieldType = defaultFieldType != null
                ? defaultFieldType
                : FieldType.UNKNOWN;

        this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping != null
                ? fieldNameToFieldTypeMapping
                : Collections.emptyMap();

        this.aggregationAllowDiskUse = aggregationAllowDiskUse;
        this.aggregationBatchSize = aggregationBatchSize;

        this.processors = List.of(
                new FromSubQueryProcessor(),
                new DistinctProcessor(),
                new GroupByProcessor(),
                new TotalGroupProcessor(),
                new ProjectionProcessor(),
                new CountAllProcessor(),
                new JoinProcessor(),
                new OrderByProcessor(),
                new WhereProcessor(this.defaultFieldType, this.fieldNameToFieldTypeMapping, false, new AliasHolder()),
                new HavingProcessor(this.defaultFieldType, this.fieldNameToFieldTypeMapping, true, new AliasHolder())
        );

        this.logQueryEnabled = logQueryEnabled != null
                ? logQueryEnabled
                : true;
    }

    /**
     * Converts a single SQL query into a MongoDB query.
     *
     * @param statement SQL query as a {@link Statement}.
     * @return The resulting mongo query.
     * @throws QueryConversionException if parsing or conversion fails.
     */
    public String convert(@NonNull Statement statement) throws QueryConversionException {
        UUID convertId = UUID.randomUUID();
        long startTime = System.nanoTime();

        try {
            SQLCommandInfo sqlCommandInfo = SQLCommandInfo.builderWithDefaults(this.defaultFieldType, this.fieldNameToFieldTypeMapping)
                    .build()
                    .fromStatement(statement);

            validate(sqlCommandInfo);

            MongoQueryHolder queryHolder = getMongoQueryInternal(sqlCommandInfo);
            queryHolder.getFilterWrapper().setOffset(sqlCommandInfo.getOffset());
            queryHolder.getFilterWrapper().setLimit(sqlCommandInfo.getLimit());

            String mongoQuery = MongoQueryFormatter.formatQuery(
                    queryHolder,
                    getSqlCommandInfo(statement),
                    getAggregationAllowDiskUse(),
                    getAggregationBatchSize()
            );

            printQuery(convertId, "Original SQL Query",      statement.toString());
            printQuery(convertId, "Converted MongoDB Query", mongoQuery);

            printConversionDuration(convertId, System.nanoTime() - startTime);

            return mongoQuery;
        } catch (ParseException e) {
            LOGGER.error("[convertId={}]: {}", convertId, e.getMessage());
            throw new QueryConversionException(e);
        }
    }

    /**
     * Internal method to validate SQLCommandInfo.
     *
     * @param sqlCommandInfo the {@link SQLCommandInfo}
     * @throws QueryConversionException if parsing errors occur
     */
    private void validate(SQLCommandInfo sqlCommandInfo) throws QueryConversionException {
        List<SelectItem<?>> selectItems = sqlCommandInfo.getSelectItems();

        List<SelectItem<?>> filteredItems = selectItems.stream()
                .filter(selectItem -> selectItem.getExpression() != null)
                .toList();

        ValidationUtils.validateFalse(
                (selectItems.size() > 1 || ValidationUtils.isSelectAllQuery(selectItems)) && sqlCommandInfo.isDistinct(),
                "Cannot run DISTINCT on more than one column."
        );

        ValidationUtils.validateFalse(
                sqlCommandInfo.getGroupByFields().isEmpty()
                        && selectItems.size() != filteredItems.size()
                        && !ValidationUtils.isSelectAllQuery(selectItems)
                        && !ValidationUtils.isCountAllQuery(selectItems)
                        && !sqlCommandInfo.isTotalGroup(),
                "Illegal expressions found in SELECT clause. Only column names are supported."
        );
    }

    /**
     * Internal method to process SQLCommandInfo into MongoQueryHolder.
     *
     * @param sqlCommandInfo the {@link SQLCommandInfo}
     * @return the {@link MongoQueryHolder}
     * @throws QueryConversionException if parsing errors occur
     */
    private MongoQueryHolder getMongoQueryInternal(SQLCommandInfo sqlCommandInfo) throws QueryConversionException {
        MongoQueryHolder mongoQueryHolder = new MongoQueryHolder(
                sqlCommandInfo.getBaseTableName(),
                sqlCommandInfo.getSqlCommandType()
        );

        for (QueryProcessor processor : processors) {
            processor.process(sqlCommandInfo, mongoQueryHolder);
        }

        mongoQueryHolder.getFilterWrapper().setOffset(sqlCommandInfo.getOffset());
        mongoQueryHolder.getFilterWrapper().setLimit(sqlCommandInfo.getLimit());

        return mongoQueryHolder;
    }

    private SQLCommandInfo getSqlCommandInfo(Statement statement)
            throws QueryConversionException, ParseException {

        return SQLCommandInfo.builderWithDefaults(this.defaultFieldType, this.fieldNameToFieldTypeMapping)
                .build()
                .fromStatement(statement);
    }

    /**
     * Logs information about SQL or MongoDB queries.
     *
     * @param convertId The unique conversion ID for tracking.
     * @param message   message of the log.
     * @param query     The actual query string.
     */
    private void printQuery(UUID convertId, String message, String query) {
        if (!logQueryEnabled) {
            return;
        }

        LOGGER.info("[convertId={}] {}:\n{}", convertId, message, query);
    }


    /**
     * Logs the successful completion of a query conversion.
     *
     * @param convertId   The unique conversion ID for tracking.
     * @param elapsedTime The duration time of the conversion in nanoseconds.
     */
    private void printConversionDuration(UUID convertId, long elapsedTime) {
        if (!logQueryEnabled) {
            return;
        }

        double durationInMillis = elapsedTime / 1_000_000.0;
        LOGGER.info("[convertId={}] Query conversion in {} ms", convertId, durationInMillis);
    }
}
