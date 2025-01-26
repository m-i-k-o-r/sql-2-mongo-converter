package com.koroli.queryconverter.holders;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.model.FieldType;
import lombok.Getter;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Select;

import java.util.HashMap;
import java.util.Map;

/**
 * Class that holds information about the FROM section of the query.
 */
@Getter
public class FromInfo implements Holder {

    private final FieldType defaultFieldType;
    private final Map<String, FieldType> fieldNameToFieldTypeMapping;

    private FromItem baseFrom;
    private String baseAlias;
    private final Map<String, FromItem> aliasToTable = new HashMap<>();
    private final Map<FromItem, String> tableToAlias = new HashMap<>();
    private final Map<FromItem, Holder> fromToSQLHolder = new HashMap<>();

    /**
     * Default Constructor.
     *
     * @param defaultFieldType            the default {@link FieldType}
     * @param fieldNameToFieldTypeMapping the mapping from field name to {@link FieldType}
     */
    public FromInfo(
            final FieldType defaultFieldType,
            final Map<String, FieldType> fieldNameToFieldTypeMapping
    ) {
        this.defaultFieldType = defaultFieldType;
        this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
    }

    /**
     * Add information from the From clause of this query.
     *
     * @param from  the {@link FromItem}
     * @param alias the alias
     * @throws QueryConversionException if there is an issue processing components from the sql statement
     * @throws ParseException           if there is a problem processing the {@link FromItem}
     */
    public void addFrom(final FromItem from, final String alias) throws QueryConversionException, ParseException {
        if (baseFrom != null) {
            if (alias != null) {
                aliasToTable.put(alias, from);
            }
            tableToAlias.put(from, alias);
            addToSQLHolderMap(from);
        } else {
            addBaseFrom(from, alias);
        }
    }

    /**
     * Get the table name from the base from.
     *
     * @return the table name from the base from.
     */
    @Override
    public String getBaseTableName() {
        return fromToSQLHolder.get(baseFrom).getBaseTableName();
    }

    /**
     * get the {@link Holder} from base {@link FromItem}.
     *
     * @return the {@link Holder} from base {@link FromItem}
     */
    public Holder getBaseSQLHolder() {
        return fromToSQLHolder.get(baseFrom);
    }

    /**
     * get the {@link Holder} from the provided {@link FromItem}.
     *
     * @param fromItem the {@link FromItem} from the sql query
     * @return the {@link Holder} from the provided {@link FromItem}
     */
    public Holder getSQLHolder(final FromItem fromItem) {
        return fromToSQLHolder.get(fromItem);
    }

    private void addBaseFrom(final FromItem from, final String alias) throws QueryConversionException, ParseException {
        addBaseFrom(from);
        if (alias != null) {
            baseAlias = alias;
            aliasToTable.put(alias, from);
        }
        tableToAlias.put(from, alias);
    }

    private void addBaseFrom(final FromItem from) throws QueryConversionException, ParseException {
        baseFrom = from;
        addToSQLHolderMap(from);
    }

    private void addToSQLHolderMap(final FromItem from) throws QueryConversionException, ParseException {
        if (from instanceof Table table) {
            fromToSQLHolder.put(table, new TableInfo(table.getName()));
        } else if (from instanceof Select select) {
            fromToSQLHolder.put(
                    from,
                    SQLCommandInfo
                            .builderWithDefaults(defaultFieldType, fieldNameToFieldTypeMapping).build()
                            .fromStatement(select)
            );

        }
    }
}
