package com.koroli.queryconverter.utils;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.operators.date.DateOperator;
import com.koroli.queryconverter.operators.object.ObjectOperator;
import com.koroli.queryconverter.operators.regex.RegexOperator;
import com.koroli.queryconverter.processors.WhereProcessor;
import lombok.experimental.UtilityClass;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import org.bson.Document;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Utility class for handling SQL functions and operators.
 */
@UtilityClass
public class FunctionUtils {

    private static final String REGEX_MATCH = "regexMatch";
    private static final String NOT_REGEX_MATCH = "notRegexMatch";

    /**
     * Identifies if the given expression is an {@link ObjectOperator}.
     *
     * @param whereProcessor the {@link WhereProcessor}.
     * @param expression     the expression to evaluate.
     * @return the {@link ObjectOperator} or {@code null} if not applicable.
     */
    public static ObjectOperator identifyObjectOperator(
            WhereProcessor whereProcessor,
            Expression expression
    ) {
        return switch (expression) {
            case ComparisonOperator comparisonOperator   -> handleComparisonOperator(comparisonOperator);
            case InExpression inExpression               -> handleInExpression(whereProcessor, inExpression);
            case Function function                       -> handleStandaloneFunction(function);
            default -> null;
        };
    }

    private static ObjectOperator handleComparisonOperator(ComparisonOperator comparisonOperator) {
        String rightExpression = ParsingUtils.extractStringValue(comparisonOperator.getRightExpression());

        if (comparisonOperator.getLeftExpression() instanceof Function function) {
            String functionName = function.getName();
            if ("toobjectid".equalsIgnoreCase(functionName) || "objectid".equals(functionName)) {
                String column = ParsingUtils.extractStringValue(function.getParameters().getExpressions().get(0));
                return ObjectOperator.builder()
                        .column(column)
                        .value(rightExpression)
                        .comparisonExpression(comparisonOperator)
                        .build();
            }
        } else if (comparisonOperator.getRightExpression() instanceof Function function) {
            String functionName = ParsingUtils.translateFunctionName(function.getName());
            if ("toobjectid".equalsIgnoreCase(functionName)) {
                String column = ParsingUtils.extractStringValue(comparisonOperator.getLeftExpression());
                String value = ParsingUtils.extractStringValue(function.getParameters().getExpressions().get(0));
                return ObjectOperator.builder()
                        .column(column)
                        .value(value)
                        .comparisonExpression(comparisonOperator)
                        .build();
            }
        }
        return null;
    }

    private static ObjectOperator handleInExpression(
            WhereProcessor whereProcessor,
            InExpression inExpression
    ) {
        Expression leftExpression = inExpression.getLeftExpression();

        if (leftExpression instanceof Function function) {
            if ("objectid".equalsIgnoreCase(function.getName())
                    && function.getParameters() instanceof ExpressionList<?> expressionList
                    && expressionList.getExpressions().size() == 1
                    && expressionList.getExpressions().getFirst() instanceof StringValue stringValue) {

                String column = ParsingUtils.extractStringValue(stringValue);
                if (inExpression.getRightExpression() instanceof ExpressionList<?> rightExpressions) {
                    List<Object> parsedValues = rightExpressions.getExpressions().stream()
                            .map(expression -> parseExpressionSafe(whereProcessor, expression, leftExpression))
                            .toList();

                    return ObjectOperator.builder()
                            .column(column)
                            .value(parsedValues)
                            .comparisonExpression(inExpression)
                            .build();
                }
            }
        }
        return null;
    }

    private static Object parseExpressionSafe(
            WhereProcessor processor,
            Expression expression,
            Expression context
    ) {
        try {
            return processor.parseExpression(new Document(), expression, context);
        } catch (QueryConversionException e) {
            throw new RuntimeException(e);
        }
    }

    private static ObjectOperator handleStandaloneFunction(Function function) {
        String functionName = ParsingUtils.translateFunctionName(function.getName());
        if ("toobjectid".equalsIgnoreCase(functionName)) {
            String value = ParsingUtils.extractStringValue(function.getParameters().getExpressions().getFirst());
            return ObjectOperator.builder()
                    .column(null)
                    .value(value)
                    .comparisonExpression(new EqualsTo())
                    .build();
        }
        return null;
    }

    /**
     * Determines if the given expression is a {@link DateOperator}.
     *
     * @param expression the expression to evaluate.
     * @return the {@link DateOperator} or {@code null} if not applicable.
     * @throws QueryConversionException if the expression cannot be parsed.
     */
    public static DateOperator identifyDateOperator(Expression expression) throws QueryConversionException {
        if (expression instanceof ComparisonOperator comparisonOperator) {
            String rightExpression = ParsingUtils.extractStringValue(comparisonOperator.getRightExpression());

            if (comparisonOperator.getLeftExpression() instanceof Function function
                    && "date".equalsIgnoreCase(function.getName())
                    && function.getParameters() instanceof ExpressionList<?> expressionList
                    && expressionList.size() == 2
                    && expressionList.get(1) instanceof StringValue stringValue
            ) {
                String column = ParsingUtils.extractStringValue(expressionList.getExpressions().getFirst());
                return DateOperator.builder()
                        .format(stringValue.getValue())
                        .value(rightExpression)
                        .column(column)
                        .operator(comparisonOperator)
                        .build();
            }
        }
        return null;
    }

    /**
     * Checks if the expression is a regex operator and returns the corresponding {@link RegexOperator}.
     *
     * @param expression the expression to evaluate.
     * @return the {@link RegexOperator} or {@code null} if not applicable.
     * @throws QueryConversionException if the regex is invalid.
     */
    public static RegexOperator identifyRegexOperator(Expression expression) throws QueryConversionException {
        if (expression instanceof EqualsTo equalsTo) {
            String rightExpression = equalsTo.getRightExpression().toString();

            if (equalsTo.getLeftExpression() instanceof Function function) {
                String functionName = function.getName();

                if ((REGEX_MATCH.equalsIgnoreCase(functionName)
                        || NOT_REGEX_MATCH.equalsIgnoreCase(functionName))
                        && (function.getParameters().getExpressions().size() == 2
                        || function.getParameters().getExpressions().size() == 3)
                        && function.getParameters().getExpressions().get(1) instanceof StringValue
                ) {
                    boolean rightExpressionValue = Boolean.parseBoolean(rightExpression);
                    ValidationUtils.validateTrue(rightExpressionValue, "False is not allowed for regexMatch function");
                    return getRegexOperator(function, NOT_REGEX_MATCH.equalsIgnoreCase(function.getName()));
                }
            }
        } else if (expression instanceof Function function) {
            if ((REGEX_MATCH.equalsIgnoreCase(function.getName())
                    || NOT_REGEX_MATCH.equalsIgnoreCase(function.getName()))
                    && (function.getParameters().getExpressions().size() == 2
                    || function.getParameters().getExpressions().size() == 3)
                    && function.getParameters().getExpressions().get(1) instanceof StringValue
            ) {
                return getRegexOperator(function, NOT_REGEX_MATCH.equalsIgnoreCase(function.getName()));
            }
        }
        return null;
    }

    private static RegexOperator getRegexOperator(Function function, boolean isNot) throws QueryConversionException {
        String column = ParsingUtils.extractStringValue(function.getParameters().getExpressions().get(0));
        String regex = NormalizationUtils.sanitizeString(((StringValue) function.getParameters().getExpressions().get(1)).getValue());

        try {
            Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new QueryConversionException(e);
        }

        RegexOperator operator = RegexOperator.builder()
                .column(column)
                .regex(regex)
                .isNot(isNot)
                .build();

        if (function.getParameters().getExpressions().size() == 3
                && function.getParameters().getExpressions().get(2) instanceof StringValue stringValue
        ) {
            operator = operator.withOptions(stringValue.getValue());
        }

        return operator;
    }

    /**
     * Generates an aggregation field name based on the function and alias.
     *
     * @param function the function to process.
     * @param alias    the alias for the field.
     * @return a mapping of the field name to the alias.
     * @throws QueryConversionException if the function cannot be processed.
     */
    public static Map.Entry<String, String> generateAggregationField(
            Function function,
            Alias alias
    ) throws QueryConversionException {

        String aliasName = alias == null
                ? null
                : alias.getName();

        return generateAggregationField(function, aliasName);
    }

    /**
     * Generates an aggregation field name based on the function and alias.
     *
     * @param function the function to process.
     * @param alias    the alias for the field.
     * @return a mapping of the field name to the alias.
     * @throws QueryConversionException if the function cannot be processed.
     */
    public static Map.Entry<String, String> generateAggregationField(
            Function function,
            String alias
    ) throws QueryConversionException {

        String field = ParsingUtils.extractFieldFromFunction(function);
        String functionName = function.getName().toLowerCase();

        if ("*".equals(field) || "count".equals(functionName)) {
            return new AbstractMap.SimpleEntry<>(field, alias == null ? functionName : alias);
        }

        String generatedAlias = alias == null
                ? functionName + "_" + field.replaceAll("\\.", "_")
                : alias;

        return new AbstractMap.SimpleEntry<>(field, generatedAlias);
    }
}
