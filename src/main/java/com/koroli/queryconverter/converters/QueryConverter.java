package com.koroli.queryconverter.converters;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.AliasHolder;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.model.FieldType;
import com.koroli.queryconverter.processors.*;
import com.koroli.queryconverter.query.MongoQueryHolder;
import com.koroli.queryconverter.utils.ValidationUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.Token;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Main class responsible for query conversion.
 */
@Getter
public final class QueryConverter {
    private final InputStream inputStream;

    private final Map<String, FieldType> fieldNameToFieldTypeMapping;
    private final FieldType defaultFieldType;

    private final Boolean aggregationAllowDiskUse;
    private final Integer aggregationBatchSize;
    private final CCJSqlParser jSqlParser;
    private final SQLCommandInfo sqlCommandInfo;
    private final MongoQueryHolder queryHolder;

    private final List<QueryProcessor> processors;

    /**
     * Initializes a QueryConverter instance and processes the SQL input.
     *
     * @throws QueryConversionException if initialization or parsing fails.
     */
    @Builder
    public QueryConverter(
            @NonNull String sqlQuery,
            Map<String, FieldType> fieldNameToFieldTypeMapping,
            FieldType defaultFieldType,
            Boolean aggregationAllowDiskUse,
            Integer aggregationBatchSize
    ) throws QueryConversionException {
        try {
            this.inputStream = new ByteArrayInputStream(sqlQuery.getBytes(StandardCharsets.UTF_8));

            this.aggregationAllowDiskUse = aggregationAllowDiskUse;
            this.aggregationBatchSize = aggregationBatchSize;

            this.defaultFieldType = defaultFieldType != null
                    ? defaultFieldType
                    : FieldType.UNKNOWN;

            this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping != null
                    ? fieldNameToFieldTypeMapping
                    : Collections.emptyMap();

            this.jSqlParser = CCJSqlParserUtil.newParser(inputStream);
            this.sqlCommandInfo = SQLCommandInfo.builderWithDefaults(this.defaultFieldType, this.fieldNameToFieldTypeMapping)
                    .build()
                    .fromStatement(jSqlParser.Statement());

            Token nextToken = jSqlParser.getNextToken();
            ValidationUtils.validateTrue(
                    nextToken.image.isEmpty() || ";".equals(nextToken.image),
                    "Unable to parse complete SQL string. Ensure no double equals (==) is used."
            );

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
                    new HavingProcessor(this.defaultFieldType, this.fieldNameToFieldTypeMapping, true, this.sqlCommandInfo.getAliasHolder())
            );
            this.queryHolder = getMongoQueryInternal(this.sqlCommandInfo);

            validate();
        } catch (IOException | ParseException e) {
            throw new QueryConversionException(e);
        }
    }

    private void validate() throws QueryConversionException {
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
     * Internal method to generate MongoDBQueryHolder from SQLCommandInfo.
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

    public MongoQueryHolder getResultQuery() {
        return this.queryHolder;
    }
}

