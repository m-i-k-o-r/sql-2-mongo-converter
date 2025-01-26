package com.koroli.queryconverter.utils;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.AliasHolder;
import com.koroli.queryconverter.model.FieldType;
import lombok.experimental.UtilityClass;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for normalizing data and expressions in SQL queries.
 */
@UtilityClass
public class NormalizationUtils {

    /**
     * Negative number sign for handling signed numbers.
     */
    private static final Character NEGATIVE_SIGN = '-';

    /**
     * List of date-time formatters for parsing date strings.
     */
    private static final List<DateTimeFormatter> DATE_FORMATTERS = List.of(
            DateTimeFormatter.ISO_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd"),
            DateTimeFormatter.ofPattern("yyyyMMdd")
    );

    /**
     * Normalizes an {@link Expression} into a specific data type or value.
     *
     * @param expression       the incoming expression.
     * @param otherExpression  the counterpart expression used for type inference.
     * @param defaultFieldType the default {@link FieldType} if no type mapping is available.
     * @param fieldTypeMapping a map linking field names to their {@link FieldType}.
     * @param aliasHolder      an {@link AliasHolder} for alias resolution.
     * @param sign             the sign (positive or negative) of the number.
     * @return the normalized value.
     * @throws QueryConversionException if the expression cannot be normalized.
     */
    public static Object normalizeExpression(
            Expression expression,
            Expression otherExpression,
            FieldType defaultFieldType,
            Map<String, FieldType> fieldTypeMapping,
            AliasHolder aliasHolder,
            Character sign
    ) throws QueryConversionException {

        FieldType fieldType = otherExpression != null
                ? (fieldTypeMapping.getOrDefault(ParsingUtils.extractStringValue(otherExpression), defaultFieldType))
                : FieldType.UNKNOWN;

        return switch (expression) {
            case StringValue stringValue ->
                    normalizeValue(
                            stringValue.getValue(),
                            fieldType
                    );
            case LongValue longValue ->
                    normalizeValue(
                            applySign(longValue.getValue(), sign),
                            fieldType
                    );
            case DoubleValue doubleValue ->
                    normalizeValue(
                            applySign(doubleValue.getValue(), sign),
                            fieldType
                    );
            case TimestampValue timestampValue ->
                    normalizeValue(
                            new Date(timestampValue.getValue().getTime()),
                            fieldType
                    );
            case DateValue dateValue ->
                    normalizeValue(
                            dateValue.getValue(),
                            fieldType
                    );
            case BooleanValue booleanValue ->
                    normalizeValue(
                            booleanValue.getValue(),
                            fieldType
                    );
            case SignedExpression signedExpression ->
                    normalizeExpression(
                            signedExpression.getExpression(),
                            otherExpression,
                            defaultFieldType,
                            fieldTypeMapping,
                            aliasHolder,
                            signedExpression.getSign()
                    );
            case Column column ->
                    handleColumnNormalization(
                            column,
                            fieldType,
                            aliasHolder
                    );
            default -> null;
        };
    }

    /**
     * Normalizes a generic value to a target type based on {@link FieldType}.
     *
     * @param value     the value to normalize.
     * @param fieldType the target {@link FieldType}.
     * @return the normalized value.
     * @throws QueryConversionException if normalization fails.
     */
    public static Object normalizeValue(
            Object value,
            FieldType fieldType
    ) throws QueryConversionException {

        if (fieldType == null || FieldType.UNKNOWN.equals(fieldType)) {
            return Optional.ofNullable(convertToBoolean(value)).orElse(value);
        }
        return switch (fieldType) {
            case STRING  -> sanitizeString(value);
            case NUMBER  -> convertToNumber(value);
            case DATE    -> convertToDate(value);
            case BOOLEAN -> Boolean.valueOf(value.toString());
            default      -> throw new QueryConversionException("Unsupported field type: " + fieldType);
        };
    }

    /**
     * Applies a sign (positive or negative) to a number.
     *
     * @param number the number to modify.
     * @param sign   the sign to apply.
     * @return the signed number.
     * @throws QueryConversionException if the number type is unsupported.
     */
    private static Object applySign(
            Number number,
            Character sign
    ) throws QueryConversionException {

        if (NEGATIVE_SIGN.equals(sign)) {
            return switch (number) {
                case Integer i -> -i;
                case Long    l -> -l;
                case Double  d -> -d;
                case Float   f -> -f;
                case null, default -> throw new QueryConversionException("Invalid number type: " + number);
            };
        }
        return number;
    }

    /**
     * Handles normalization of a column, resolving aliases if applicable.
     *
     * @param column      the {@link Column} to normalize.
     * @param fieldType   the target {@link FieldType}.
     * @param aliasHolder the {@link AliasHolder} for alias resolution.
     * @return the normalized value of the column.
     * @throws QueryConversionException if normalization fails.
     */
    private static Object handleColumnNormalization(
            Column column,
            FieldType fieldType,
            AliasHolder aliasHolder
    ) throws QueryConversionException {

        Object normalizedValue = normalizeValue(ParsingUtils.extractStringValue(column), fieldType);

        if (aliasHolder != null
                && !aliasHolder.isEmpty()
                && normalizedValue instanceof String fieldName
                && aliasHolder.containsAliasForFieldExp(fieldName)
        ) {
            return aliasHolder.getAliasFromFieldExp(fieldName);
        }
        return normalizedValue;
    }

    /**
     * Converts an object to a boolean if possible.
     *
     * @param value the value to convert.
     * @return the boolean representation, or {@code null} if not convertible.
     */
    private static Object convertToBoolean(Object value) {
        String stringValue = value.toString();
        if ("true".equalsIgnoreCase(stringValue) || "false".equalsIgnoreCase(stringValue)) {
            return Boolean.valueOf(stringValue);
        }
        return null;
    }

    /**
     * Converts an object to a date if possible.
     *
     * @param value the value to convert.
     * @return the date representation.
     * @throws QueryConversionException if conversion fails.
     */
    private static Object convertToDate(Object value) throws QueryConversionException {
        if (value instanceof String stringValue) {
            for (DateTimeFormatter formatter : DATE_FORMATTERS) {
                try {
                    LocalDateTime dateTime = LocalDateTime.parse(stringValue, formatter);
                    return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
                } catch (Exception ignored) {
                    // noop
                }
            }
            throw new QueryConversionException("Invalid date format: " + stringValue);
        }
        throw new QueryConversionException("Cannot convert value to date: " + value);
    }

    List<java.util.function.Function<String, ?>> parsers = List.of(
            Long::parseLong,
            Double::parseDouble,
            Float::parseFloat
    );

    /**
     * Converts an object to a number if possible.
     *
     * @param value the value to convert.
     * @return the numeric representation.
     * @throws QueryConversionException if conversion fails.
     */
    private static Object convertToNumber(Object value) throws QueryConversionException {
        if (value instanceof String stringValue) {
            for (var parser : parsers) {
                try {
                    return parser.apply(stringValue);
                } catch (NumberFormatException ignored) {
                    // noop
                }
            }
            throw new QueryConversionException("Invalid number format: " + value);
        }
        return value;
    }

    /**
     * Sanitizes a string by replacing double single quotes with a single quote.
     *
     * @param value the string to sanitize.
     * @return the sanitized string.
     */
    public static String sanitizeString(Object value) {
        return value.toString().replace("''", "'");
    }
}
