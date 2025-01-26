package com.koroli.queryconverter.holders;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.Map;

/**
 * Holds mappings between fields and their aliases in a SQL query.
 */
@Getter
@RequiredArgsConstructor
public class AliasHolder {

    /**
     * Maps field expressions to their aliases.
     */
    private final Map<String, String> aliasFromFieldHash;

    /**
     * Maps aliases back to their original field expressions.
     */
    private final Map<String, String> fieldFromAliasHash;

    /**
     * Creates an empty {@link AliasHolder}.
     */
    public AliasHolder() {
        this(Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Retrieves the alias for a given field expression.
     *
     * @param field the field expression for which to retrieve the alias.
     * @return the alias corresponding to the field expression, or {@code null} if no alias exists.
     */
    public String getAliasFromFieldExp(String field) {
        return aliasFromFieldHash.get(field);
    }

    /**
     * Checks if an alias exists for a given field expression.
     *
     * @param field the field expression to check.
     * @return {@code true} if an alias exists for the field, otherwise {@code false}.
     */
    public boolean containsAliasForFieldExp(String field) {
        return aliasFromFieldHash.containsKey(field);
    }

    /**
     * Retrieves the original field expression for a given alias.
     *
     * @param fieldOrAlias the alias or field expression.
     * @return the original field expression if the input is an alias, otherwise returns the input itself.
     * @throws QueryConversionException if the input is ambiguous (maps to both a field and an alias).
     */
    public String getFieldFromAliasOrField(String fieldOrAlias) throws QueryConversionException {
        if (!isAmbiguous(fieldOrAlias)) {
            String field = fieldFromAliasHash.get(fieldOrAlias);
            if (field == null) {
                return fieldOrAlias;
            } else {
                return field;
            }
        } else {
            throw new QueryConversionException("Ambiguous field: " + fieldOrAlias);
        }
    }

    /**
     * Checks if there are no aliases defined in this holder.
     *
     * @return {@code true} if no aliases are defined, otherwise {@code false}.
     */
    public boolean isEmpty() {
        return aliasFromFieldHash.isEmpty();
    }

    /**
     * Checks if a given input is ambiguous.
     * An input is ambiguous if it exists as both a field and an alias with different mappings.
     *
     * @param fieldOrAlias the input to check.
     * @return {@code true} if the input is ambiguous, otherwise {@code false}.
     */
    private boolean isAmbiguous(String fieldOrAlias) {
        String aliasFromField = aliasFromFieldHash.get(fieldOrAlias);
        String fieldFromAlias = fieldFromAliasHash.get(fieldOrAlias);
        return aliasFromField != null && fieldFromAlias != null && !aliasFromField.equals(fieldFromAlias);
    }
}
