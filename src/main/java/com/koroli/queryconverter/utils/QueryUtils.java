package com.koroli.queryconverter.utils;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import lombok.experimental.UtilityClass;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for query-related operations
 */
@UtilityClass
public class QueryUtils {

    /**
     * Regular expression pattern for LIKE range conversions
     */
    private static final Pattern LIKE_RANGE_PATTERN = Pattern.compile("(\\[.+?\\])");

    /**
     * Converts a SQL LIKE query pattern into an equivalent regex format
     *
     * @param likePattern the SQL LIKE pattern
     * @return a regex pattern equivalent to the provided LIKE pattern
     */
    public static String convertLikeToRegex(String likePattern) {
        String convertedPattern = likePattern.replace("%", ".*").replace("_", ".");
        Matcher matcher = LIKE_RANGE_PATTERN.matcher(convertedPattern);

        StringBuilder regexBuilder = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(regexBuilder, matcher.group(1) + "{1}");
        }
        matcher.appendTail(regexBuilder);

        return regexBuilder.toString();
    }

    /**
     * Retrieves the limit value from a {@link Limit} object as a long
     *
     * @param limit the {@link Limit} object
     * @return the limit value as a long, or -1 if no limit is set
     * @throws QueryConversionException if the limit value cannot be parsed
     */
    public static long extractLimitAsLong(Limit limit) throws QueryConversionException {
        return limit != null
                ? parseLongIfInteger(ParsingUtils.extractStringValue(limit.getRowCount()))
                : -1;
    }

    /**
     * Parses a string into a long value if it represents an integer
     *
     * @param stringValue the string to parse
     * @return the parsed long value
     * @throws QueryConversionException if the string represents a value exceeding the integer range
     */
    private static long parseLongIfInteger(String stringValue) throws QueryConversionException {
        BigInteger bigInt = new BigInteger(stringValue);
        ValidationUtils.validateFalse(
                bigInt.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0,
                stringValue + ": value is too large");
        return bigInt.longValue();
    }

    /**
     * Retrieves the offset value from an {@link Offset} object as a long
     *
     * @param offset the {@link Offset} object
     * @return the offset value as a long, or -1 if no offset is set
     */
    public static long extractOffsetAsLong(Offset offset) {
        return (offset != null && offset.getOffset() instanceof LongValue )
                        ? ((LongValue) offset.getOffset()).getValue()
                        : -1;
    }

    /**
     * Extracts the column references used in the GROUP BY clause from a {@link PlainSelect} object
     *
     * @param plainSelect the {@link PlainSelect} object
     * @return a list of column references used in the GROUP BY clause, or an empty list if none exist
     */
    public static List<String> extractGroupByColumns(PlainSelect plainSelect) {
        if (plainSelect.getGroupBy() == null) {
            return Collections.emptyList();
        }

        return plainSelect.getGroupBy().getGroupByExpressions().stream()
                .map(expression -> ParsingUtils.extractStringValue((Expression) expression))
                .toList();
    }
}
