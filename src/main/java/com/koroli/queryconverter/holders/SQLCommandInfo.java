package com.koroli.queryconverter.holders;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.model.FieldType;
import com.koroli.queryconverter.model.SQLCommandType;
import com.koroli.queryconverter.utils.ExpressionUtils;
import com.koroli.queryconverter.utils.QueryUtils;
import com.koroli.queryconverter.utils.ValidationUtils;
import com.koroli.queryconverter.visitors.AliasCleanerVisitor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds detailed information about the structure and components of an SQL query.
 * This class is immutable and uses a builder for object creation.
 */
// this classes so bad... I'm tired... sry
@Getter
@Builder(toBuilder = true)
public final class SQLCommandInfo implements Holder {

    // ----=== Query Metadata ===----
    private final SQLCommandType sqlCommandType;
    private final boolean isDistinct;
    private final boolean isCountAll;
    private final boolean isTotalGroup;

    // ----=== Query Components ===----
    private final FromInfo from;
    private final Expression whereClause;
    private final Expression havingClause;
    private final AliasHolder aliasHolder;
    private final List<SelectItem<?>> selectItems;
    private final List<Join> joins;
    private final List<String> groupByFields;
    private final List<OrderByElement> orderByElements;
    private final List<UpdateSet> updateSets;

    // ----=== SQL-Specific Details ===----
    private final long limit;
    private final long offset;
    private final FieldType defaultFieldType;
    private final Map<String, FieldType> fieldNameToFieldTypeMapping;


    /**
     * Retrieves the name of the base table from the query.
     *
     * @return the base table name as a {@link String}.
     */
    public String getBaseTableName() {
        return from.getBaseTableName();
    }

    /**
     * Factory method to initialize a builder with default field type and mapping.
     *
     * @param defaultFieldType            the default {@link FieldType}.
     * @param fieldNameToFieldTypeMapping the mapping of field names to their {@link FieldType}.
     * @return a preconfigured builder instance.
     */
    public static SQLCommandInfoBuilder builderWithDefaults(
            @NonNull FieldType defaultFieldType,
            @NonNull Map<String, FieldType> fieldNameToFieldTypeMapping
    ) {
        return SQLCommandInfo.builder()
                .defaultFieldType(defaultFieldType)
                .fieldNameToFieldTypeMapping(fieldNameToFieldTypeMapping);
    }

    /**
     * Processes the provided SQL statement and creates a new {@link SQLCommandInfo}.
     *
     * @param statement the SQL statement to process.
     * @return a new {@link SQLCommandInfo}.
     * @throws ParseException           if parsing the statement fails.
     * @throws QueryConversionException if conversion fails.
     */
    public SQLCommandInfo fromStatement(Statement statement)
            throws ParseException, QueryConversionException {
        return switch (statement) {
            case Select selectStmt -> this.withSelect(selectStmt);
            case Delete deleteStmt -> this.withDelete(deleteStmt);
            case Update updateStmt -> this.withUpdate(updateStmt);
            case Insert insertStmt -> this.withInsert(insertStmt);
            default -> throw new ParseException("Unsupported statement type");
        };
    }

    /**
     * Creates a new {@link SQLCommandInfo} based on a SELECT statement.
     */
    public SQLCommandInfo withSelect(Select selectStmt)
            throws ParseException, QueryConversionException {

        if (selectStmt instanceof PlainSelect plainSelect) {
            return this.withPlainSelect(plainSelect);
        } else if (selectStmt instanceof SetOperationList setOpList) {
            if (setOpList.getSelect(0) instanceof PlainSelect plainSelect) {
                return this.withPlainSelect(plainSelect);
            } else {
                throw new ParseException("Unsupported SetOperationList structure");
            }
        } else {
            throw new ParseException("Unsupported SELECT statement structure");
        }
    }

    /**
     * Creates a new {@link SQLCommandInfo} based on a DELETE statement.
     */
    public SQLCommandInfo withDelete(Delete delete)
            throws ParseException, QueryConversionException {

        ValidationUtils.validateTrue(delete.getTables().isEmpty(), "Only one table can be deleted at a time.");

        FromInfo holder = generateFromInfo(
                new FromInfo(defaultFieldType, fieldNameToFieldTypeMapping),
                delete.getTable(),
                delete.getJoins());

        return this.toBuilder()
                .sqlCommandType(SQLCommandType.DELETE)
                .isDistinct(false)
                .isCountAll(false)
                .isTotalGroup(false)
                .from(holder)
                .whereClause(delete.getWhere())
                .havingClause(null)
                .aliasHolder(null)
                .selectItems(Collections.emptyList())
                .joins(delete.getJoins())
                .groupByFields(Collections.emptyList())
                .orderByElements(delete.getOrderByElements())
                .updateSets(Collections.emptyList())
                .limit(QueryUtils.extractLimitAsLong(delete.getLimit()))
                .offset(0)
                .build();
    }

    /**
     * Creates a new {@link SQLCommandInfo} based on an UPDATE statement.
     */
    public SQLCommandInfo withUpdate(Update update)
            throws ParseException, QueryConversionException {

        ValidationUtils.validateTrue(update.getTable() != null, "An update must specify a table.");

        FromInfo holder = generateFromInfo(
                new FromInfo(defaultFieldType, fieldNameToFieldTypeMapping),
                update.getTable(),
                update.getJoins());

        return this.toBuilder()
                .sqlCommandType(SQLCommandType.UPDATE)
                .isDistinct(false)
                .isCountAll(false)
                .isTotalGroup(false)
                .from(holder)
                .whereClause(update.getWhere())
                .havingClause(null)
                .aliasHolder(null)
                .selectItems(Collections.emptyList())
                .joins(update.getJoins())
                .groupByFields(Collections.emptyList())
                .orderByElements(Collections.emptyList())
                .updateSets(update.getUpdateSets())
                .limit(QueryUtils.extractLimitAsLong(update.getLimit()))
                .offset(0)
                .build();
    }

    /**
     * Creates a new {@link SQLCommandInfo} based on an INSERT statement.
     */
    public SQLCommandInfo withInsert(Insert insert)
            throws ParseException, QueryConversionException {

        ValidationUtils.validateTrue(insert.getTable() != null, "An insert must specify a table.");

        FromInfo holder = generateFromInfo(
                new FromInfo(defaultFieldType, fieldNameToFieldTypeMapping),
                insert.getTable(),
                null);

        return this.toBuilder()
                .sqlCommandType(SQLCommandType.INSERT)
                .isDistinct(false)
                .isCountAll(false)
                .isTotalGroup(false)
                .from(holder)
                .whereClause(null)
                .havingClause(null)
                .aliasHolder(null)
                .selectItems(Collections.emptyList())
                .joins(Collections.emptyList())
                .groupByFields(Collections.emptyList())
                .orderByElements(Collections.emptyList())
                .updateSets(Collections.emptyList())
                .limit(0)
                .offset(0)
                .build();
    }

    /**
     * Creates a new {@link SQLCommandInfo} based on a PlainSelect statement.
     */
    private SQLCommandInfo withPlainSelect(PlainSelect plainSelect) throws ParseException, QueryConversionException {

        ValidationUtils.validateTrue(plainSelect != null, "The PlainSelect statement cannot be null.");
        ValidationUtils.validateTrue(plainSelect.getFromItem() != null, "The FROM item cannot be null.");

        FromInfo holder = generateFromInfo(
                new FromInfo(defaultFieldType, fieldNameToFieldTypeMapping),
                plainSelect.getFromItem(),
                plainSelect.getJoins());

        return this.toBuilder()
                .sqlCommandType(SQLCommandType.SELECT)
                .isDistinct(plainSelect.getDistinct() != null)
                .isCountAll(ValidationUtils.isCountAllQuery(plainSelect.getSelectItems()))
                .isTotalGroup(ValidationUtils.isTotalGroup(plainSelect.getSelectItems()))
                .from(holder)
                .whereClause(plainSelect.getWhere())
                .havingClause(plainSelect.getHaving())
                .aliasHolder(generateHashAliasFromSelectItems(plainSelect.getSelectItems(), holder))
                .selectItems(plainSelect.getSelectItems())
                .joins(plainSelect.getJoins())
                .groupByFields(QueryUtils.extractGroupByColumns(plainSelect))
                .orderByElements(plainSelect.getOrderByElements())
                .updateSets(Collections.emptyList())
                .limit(QueryUtils.extractLimitAsLong(plainSelect.getLimit()))
                .offset(QueryUtils.extractOffsetAsLong(plainSelect.getOffset()))
                .build();
    }

    private FromInfo generateFromInfo(FromInfo holder, FromItem fromItem, List<Join> joins)
            throws ParseException, QueryConversionException {
        FromInfo result = holder;
        Alias alias = fromItem.getAlias();
        result.addFrom(fromItem, alias != null ? alias.getName() : null);

        if (joins != null) {
            for (Join join : joins) {
                ExpressionUtils.setInnerJoinIfDefault(join);
                if (join.isInner() || join.isLeft()) {
                    result = generateFromInfo(result, join.getRightItem(), null);
                } else {
                    throw new ParseException("Unsupported join type");
                }
            }
        }
        return result;
    }

    private AliasHolder generateHashAliasFromSelectItems(List<SelectItem<?>> selectItems, FromInfo holder) {
        Map<String, String> aliasFromField = new HashMap<>();
        Map<String, String> fieldFromAlias = new HashMap<>();

        for (SelectItem<?> item : selectItems) {
            if (item.getExpression() instanceof AllColumns || item.getExpression() instanceof AllTableColumns) {
                continue;
            }

            Alias alias = item.getAlias();
            if (alias != null) {
                Expression expression = item.getExpression();
                expression.accept(
                        new AliasCleanerVisitor(holder.getBaseAlias())
                );
                String expressionStr = expression.toString();
                String aliasStr = alias.getName();

                aliasFromField.put(expressionStr, aliasStr);
                fieldFromAlias.put(aliasStr, expressionStr);
            }
        }

        return new AliasHolder(aliasFromField, fieldFromAlias);
    }
}