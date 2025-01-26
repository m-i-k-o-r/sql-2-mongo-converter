package com.koroli.queryconverter.holders;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Represents information about a table in a SQL query.
 */
@Getter
@AllArgsConstructor
public class TableInfo implements Holder {
    private String baseTable;

    /**
     * Retrieves the name of the base table.
     *
     * @return the base table name.
     */
    @Override
    public String getBaseTableName() {
        return baseTable;
    }
}
