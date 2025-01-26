package com.koroli.queryconverter.processors;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.holders.AliasHolder;
import com.koroli.queryconverter.holders.SQLCommandInfo;
import com.koroli.queryconverter.model.FieldType;
import com.koroli.queryconverter.operators.date.DateOperator;
import com.koroli.queryconverter.operators.object.ObjectOperator;
import com.koroli.queryconverter.operators.regex.RegexOperator;
import com.koroli.queryconverter.query.MongoQueryHolder;
import com.koroli.queryconverter.utils.*;
import com.koroli.queryconverter.visitors.AliasCleanerVisitor;
import com.koroli.queryconverter.visitors.WhereMatchVisitor;
import lombok.AllArgsConstructor;
import lombok.Getter;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Processes SQL WHERE clauses and converts them into MongoDB query filters.
 */
@Getter
@AllArgsConstructor
public class WhereProcessor implements QueryProcessor {

    private final FieldType defaultFieldType;
    private final Map<String, FieldType> fieldNameToFieldTypeMapping;
    private final boolean requiresMultistepAggregation;
    protected final AliasHolder aliasHolder;

    /**
     * Processes the WHERE clause of an SQL query and updates the MongoDB query filter.
     *
     * @param sqlCommandInfo the SQL command information containing the WHERE clause
     * @param queryHolder    the MongoDB query holder to store the converted filter
     * @throws QueryConversionException if an error occurs while converting the WHERE clause
     */
    @Override
    public void process(
            SQLCommandInfo sqlCommandInfo,
            MongoQueryHolder queryHolder
    ) throws QueryConversionException {

        Expression whereClause = sqlCommandInfo.getWhereClause();
        if (whereClause == null) {
            return;
        }

        if (sqlCommandInfo.getJoins() != null && !sqlCommandInfo.getJoins().isEmpty()) {
            Optional<Expression> tempExpression = Optional.empty();
            AtomicBoolean haveOrExpression = new AtomicBoolean(false);

            whereClause.accept(
                    new WhereMatchVisitor(
                            sqlCommandInfo.getFrom().getBaseAlias(),
                            tempExpression,
                            haveOrExpression
                    ));

            if (haveOrExpression.get()) {
                return;
            }

            if (tempExpression.isPresent()) {
                whereClause = tempExpression.get();
            }
        }

        whereClause.accept(
                new AliasCleanerVisitor(sqlCommandInfo.getFrom().getBaseAlias()), sqlCommandInfo.getFrom()
        );

        Document parsedQuery = (Document) parseExpression(new Document(), whereClause, null);
        queryHolder.getFilterWrapper().setQuery(parsedQuery);
    }

    /**
     * Parsing an expression from SQL to MongoDB query format.
     *
     * @param query              the MongoDB query document to update
     * @param incomingExpression the SQL expression to parse
     * @param otherSide          expression on the other side of the comparison, if applicable
     * @return an updated MongoDB query document or nested structure
     * @throws QueryConversionException if an error occurs during expression parsing
     */
    // I know this method is too huge, I couldn't split it up, sorry...
    public Object parseExpression(
            Document query,
            Expression incomingExpression,
            Expression otherSide
    ) throws QueryConversionException {

        // 1. Comparative operators
        if (incomingExpression instanceof ComparisonOperator comparisonOperator) {
            RegexOperator regexOperator = FunctionUtils.identifyRegexOperator(incomingExpression);
            DateOperator dateOperator = FunctionUtils.identifyDateOperator(incomingExpression);
            ObjectOperator objectOperator = FunctionUtils.identifyObjectOperator(this, incomingExpression);

            // 1.1 REGEX
            if (regexOperator != null) {
                Document regexDocument = new Document("$regex", regexOperator.getRegex());
                if (regexOperator.getOptions() != null) {
                    regexDocument.append("$options", regexOperator.getOptions());
                }
                query.put(regexOperator.getColumn(), applyNotOperatorIfRequired(regexDocument, regexOperator));
                return query;
            }

            // 1.2 DATE
            if (dateOperator != null) {
                query.put(
                        dateOperator.getColumn(),
                        new Document(dateOperator.getComparisonExpression(), dateOperator.getDate())
                );
                return query;
            }

            // 1.3 OBJECT_ID
            if (objectOperator != null) {
                query.put(objectOperator.getColumn(), objectOperator.toDocument());
                return query;
            }

            // 1.4 Common comparators (==, !=, <, >, <=, >=)
            Expression leftExpression = comparisonOperator.getLeftExpression();
            Expression rightExpression = comparisonOperator.getRightExpression();

            String operator = switch (incomingExpression) {
                case EqualsTo ignored            -> "eq";
                case NotEqualsTo ignored         -> "ne";
                case GreaterThan ignored         -> "gt";
                case MinorThan ignored           -> "lt";
                case GreaterThanEquals ignored   -> "gte";
                case MinorThanEquals ignored     -> "lte";
                default -> null;
            };

            if (operator != null) {
                handleComparisonOperator(query, leftExpression, rightExpression, operator);
            }

            return query;
        }

        // 2. LIKE expressions
        if (incomingExpression instanceof LikeExpression likeExpression
                && likeExpression.getLeftExpression() instanceof Column
                && (likeExpression.getRightExpression() instanceof StringValue || likeExpression.getRightExpression() instanceof Column)
        ) {

            String fieldName = ParsingUtils.extractStringValue(likeExpression.getLeftExpression());
            String rightValue = ParsingUtils.extractStringValue(likeExpression.getRightExpression());
            String regexPattern = "^" + QueryUtils.convertLikeToRegex(rightValue) + "$";

            Document value = likeExpression.isNot()
                    ? new Document(fieldName, new Document("$not", regexPattern))
                    : new Document(fieldName, new Document("$regex", regexPattern));

            query.putAll(value);
            return query;
        }

        // 3. IS NULL / IS NOT NULL
        if (incomingExpression instanceof IsNullExpression isNullExpression) {
            Expression leftExpression = isNullExpression.getLeftExpression();
            boolean isNotNull = isNullExpression.isNot();

            if (leftExpression instanceof Function) {
                Document result = (Document) processFunctionExpressions(
                        new Document(),
                        leftExpression,
                        defaultFieldType,
                        fieldNameToFieldTypeMapping);
                result.append("$exists", isNotNull);
                query.putAll(result);
            } else {
                String fieldName = ParsingUtils.extractStringValue(leftExpression);
                query.put(fieldName, new Document("$exists", isNotNull));
            }
            return query;
        }

        // 4. IN / NOT IN
        if (incomingExpression instanceof InExpression inExpression) {
            Expression leftExpression = inExpression.getLeftExpression();
            String fieldName = ParsingUtils.extractStringValue(leftExpression);

            ObjectOperator objectOperator = FunctionUtils.identifyObjectOperator(this, incomingExpression);
            if (objectOperator != null) {
                query.put(objectOperator.getColumn(), objectOperator.toDocument());
                return query;
            }

            List<Object> objectList = ((ExpressionList<?>) inExpression.getRightExpression()).stream()
                    .map(ex -> {
                        try {
                            return this.parseExpression(new Document(), ex, leftExpression);
                        } catch (QueryConversionException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toList();

            if (leftExpression instanceof Function) {
                String operator = inExpression.isNot() ? "$fnin" : "$fin";

                Document value = new Document("function", parseExpression(new Document(), leftExpression, null));
                value.append("list", objectList);

                query.put(operator, value);
            } else {
                String operator = inExpression.isNot() ? "$nin" : "$in";

                if (requiresMultistepAggregation) {
                    Document document = new Document();
                    List<Object> list = Arrays.asList(
                            ExpressionUtils.convertToNode(leftExpression, requiresMultistepAggregation),
                            objectList
                    );
                    document.put(operator, list);
                    query.put("$expr", document);
                } else {
                    Document document = new Document(operator, objectList);
                    query.put(fieldName, document);
                }
            }
            return query;
        }

        // 5. BETWEEN / NOT BETWEEN
        if (incomingExpression instanceof Between betweenExpression) {
            GreaterThanEquals start = new GreaterThanEquals();
            start.setLeftExpression(betweenExpression.getLeftExpression());
            start.setRightExpression(betweenExpression.getBetweenExpressionStart());

            MinorThanEquals end = new MinorThanEquals();
            end.setLeftExpression(betweenExpression.getLeftExpression());
            end.setRightExpression(betweenExpression.getBetweenExpressionEnd());

            Expression combined = new AndExpression(
                    betweenExpression.isNot() ? new NotExpression(start) : start,
                    betweenExpression.isNot() ? new NotExpression(end) : end
            );
            return parseExpression(query, combined, otherSide);
        }

        // 6. AND / OR
        if (incomingExpression instanceof AndExpression
                || incomingExpression instanceof OrExpression
        ) {
            processLogicalOperators(incomingExpression, query);
            return query;
        }

        // 7. NOT
        if (incomingExpression instanceof NotExpression notExpression) {
            Expression expression = notExpression.getExpression();

            if (expression instanceof Column) {
                return new Document(
                        ParsingUtils.extractStringValue(expression),
                        new Document("$ne", true)
                );
            }

            if (expression instanceof ComparisonOperator) {
                Document parsedDocument = (Document) parseExpression(new Document(), expression, otherSide);
                if (!parsedDocument.isEmpty()) {
                    Map.Entry<String, Object> entry = parsedDocument.entrySet().iterator().next();
                    String key = entry.getKey();
                    Object value = entry.getValue();

                    if (value instanceof Document docValue) {
                        return new Document(
                                key,
                                new Document("$not", docValue)
                        );
                    }
                }
            }

            return query;
        }

        // 8. Functions
        if (incomingExpression instanceof Function) {
            RegexOperator regexOperator = FunctionUtils.identifyRegexOperator(incomingExpression);
            ObjectOperator objectOperator = FunctionUtils.identifyObjectOperator(this, incomingExpression);

            if (regexOperator != null) {
                Document regexDocument = new Document("$regex", regexOperator.getRegex());
                if (regexOperator.getOptions() != null) {
                    regexDocument.append("$options", regexOperator.getOptions());
                }
                query.put(regexOperator.getColumn(), applyNotOperatorIfRequired(regexDocument, regexOperator));
                return query;
            }

            if (objectOperator != null) {
                return objectOperator.toDocument();
            }

            return processFunctionExpressions(query, incomingExpression, defaultFieldType, fieldNameToFieldTypeMapping);
        }

        if (otherSide == null) {
            return new Document(ParsingUtils.extractStringValue(incomingExpression), true);
        }

        // 10. otherwise... normalize as a regular expression
        return NormalizationUtils.normalizeExpression(
                incomingExpression,
                otherSide,
                defaultFieldType,
                fieldNameToFieldTypeMapping,
                aliasHolder,
                null
        );
    }

    /**
     * Parses simple comparative operators into MongoDB query operators.
     *
     * @param query           the MongoDB query document to update
     * @param leftExpression  the left expression of the comparison
     * @param rightExpression the right expression of the comparison
     * @param comparatorType  the type of comparison
     * @throws QueryConversionException if an error occurs during comparison parsing
     */
    private void handleComparisonOperator(
            Document query,
            Expression leftExpression,
            Expression rightExpression,
            String comparatorType
    ) throws QueryConversionException {

        String operator = "$" + comparatorType;
        Document doc;

        boolean leftIsFunction = (leftExpression instanceof Function);
        boolean rightIsFunction = (rightExpression instanceof Function);
        boolean leftIsColumn = ValidationUtils.isColumnExpression(leftExpression);
        boolean rightIsColumn = ValidationUtils.isColumnExpression(rightExpression);

        // 1) On left is function, and on right is column/value
        if (leftIsFunction) {
            Object leftParsed = parseExpression(new Document(), leftExpression, rightExpression);
            Object rightParsed = parseExpression(new Document(), rightExpression, leftExpression);

            doc = new Document(operator, Arrays.asList(
                    leftParsed,
                    (rightIsColumn && !rightExpression.toString().startsWith("$") && !(leftParsed instanceof Document))
                            ? "$" + rightParsed
                            : rightParsed
            ));

            if (requiresMultistepAggregation) {
                query.put("$expr", doc);
            } else {
                query.putAll(doc);
            }
            return;
        }

        // 2) Both are columns
        if (leftIsColumn && rightIsColumn) {
            if (requiresMultistepAggregation) {
                String leftName = ((Column) leftExpression).getName(false);
                String rightName = ((Column) rightExpression).getName(false);

                doc = new Document(
                        operator,
                        Arrays.asList(
                                leftName.startsWith("$") ? leftName : "$" + leftName,
                                rightName.startsWith("$") ? rightName : "$" + rightName
                        ));

                query.put("$expr", doc);
            } else {

                query.put(
                        parseExpression(new Document(), leftExpression, rightExpression).toString(),
                        parseExpression(new Document(), rightExpression, leftExpression)
                );
            }
            return;
        }

        // 3) On right is function
        if (rightIsFunction) {
            Object leftParsed = parseExpression(new Document(), leftExpression, rightExpression);
            Object rightParsed = parseExpression(new Document(), rightExpression, leftExpression);

            doc = new Document(
                    operator,
                    Arrays.asList(
                            leftParsed,
                            (leftIsColumn && !leftExpression.toString().startsWith("$") && !(rightParsed instanceof Document))
                                    ? "$" + rightParsed
                                    : rightParsed
                    ));

            if (requiresMultistepAggregation) {
                query.put("$expr", doc);
            } else {
                query.putAll(doc);
            }
            return;
        }

        // 4) On left is column, and on right usual value
        if (leftIsColumn) {
            String fieldName = parseExpression(new Document(), leftExpression, rightExpression).toString();
            Object rightVal = parseExpression(new Document(), rightExpression, leftExpression);

            if ("$eq".equals(operator)) {
                query.put(fieldName, rightVal);
            } else {
                query.append(fieldName, new Document(operator, rightVal));
            }
            return;
        }

        // 5) otherwise...
        Object leftParse = parseExpression(new Document(), leftExpression, rightExpression);

        doc = new Document(
                operator,
                Arrays.asList(
                        leftParse,
                        ExpressionUtils.convertToNode(rightExpression, requiresMultistepAggregation)
                ));

        if (requiresMultistepAggregation) {
            query.put("$expr", doc);
        } else {
            if ("eq".equals(comparatorType) && leftParse instanceof String) {
                query.put(
                        leftParse.toString(),
                        parseExpression(
                                new Document(),
                                rightExpression,
                                leftExpression
                        ));

            } else if (leftParse instanceof String) {
                Document subDocument = new Document(
                        operator,
                        parseExpression(
                                new Document(),
                                rightExpression,
                                leftExpression
                        ));

                query.put(leftParse.toString(), subDocument);
            } else {
                query.putAll(doc);
            }
        }
    }

    private Object applyNotOperatorIfRequired(Document regexDocument, RegexOperator regexOperator) {
        if (regexOperator.isNot()) {
            if (regexOperator.getOptions() != null) {
                throw new IllegalArgumentException("$not regex not supported with options");
            }
            return new Document("$not", regexOperator.getRegex());
        }
        return regexDocument;
    }

    /**
     * Recursively processes function calls and expressions into MongoDB query format.
     *
     * @param query                       the MongoDB query document to update
     * @param object                      the function or expression to process
     * @param defaultFieldType            the default field type for the conversion
     * @param fieldNameToFieldTypeMapping mapping of field names to field types
     * @return the processed MongoDB query document
     * @throws QueryConversionException if an error occurs during function processing
     */
    protected Object processFunctionExpressions(
            Document query,
            Object object,
            FieldType defaultFieldType,
            Map<String, FieldType> fieldNameToFieldTypeMapping
    ) throws QueryConversionException {

        if (object instanceof Function function) {
            String mongoFunctionName = ParsingUtils.translateFunctionName(function.getName());
            Object paramsResult = processFunctionExpressions(
                    new Document(),
                    function.getParameters(),
                    defaultFieldType,
                    fieldNameToFieldTypeMapping);

            query.put("$" + mongoFunctionName, paramsResult);
            return query.isEmpty()
                    ? null
                    : query;

        } else if (object instanceof ExpressionList<?> expressionList) {
            List<Object> objectList = new ArrayList<>();

            for (Object expr : expressionList.getExpressions()) {
                objectList.add(
                        processFunctionExpressions(
                                new Document(),
                                expr,
                                defaultFieldType,
                                fieldNameToFieldTypeMapping
                        ));
            }

            return (objectList.size() == 1)
                    ? objectList.getFirst()
                    : objectList;

        } else if (object instanceof Expression expression) {

            return NormalizationUtils.normalizeExpression(
                    expression,
                    null,
                    defaultFieldType,
                    fieldNameToFieldTypeMapping,
                    new AliasHolder(),
                    null
            );
        }

        return query.isEmpty()
                ? null
                : query;
    }

    private void processLogicalOperators(Expression incomingExpression, Document query) throws QueryConversionException {
        String key = (incomingExpression instanceof AndExpression) ? "$and" : "$or";

        BinaryExpression binary = (BinaryExpression) incomingExpression;
        Expression left = binary.getLeftExpression();
        Expression right = binary.getRightExpression();

        List<Object> flattened = flattenAndOr(new ArrayList<>(), binary, left, right);

        if (!flattened.isEmpty()) {
            Collections.reverse(flattened);
            query.put(key, flattened);
        } else {
            query.put(
                    key,
                    Arrays.asList(
                            parseExpression(new Document(), left, null),
                            parseExpression(new Document(), right, null)
                    ));
        }
    }

    private List<Object> flattenAndOr(
            List<Object> resultList,
            Expression topExpression,
            Expression leftExpression,
            Expression rightExpression
    ) throws QueryConversionException {
        if (topExpression.getClass().isInstance(leftExpression)
                && isLogicalExpression(leftExpression)
                && !isLogicalExpression(rightExpression)
        ) {
            BinaryExpression leftBin = (BinaryExpression) leftExpression;
            resultList.add(parseExpression(new Document(), rightExpression, null));
            return flattenAndOr(resultList, topExpression, leftBin.getLeftExpression(), leftBin.getRightExpression());

        } else if (isLogicalExpression(topExpression)
                && !isLogicalExpression(leftExpression)
                && !isLogicalExpression(rightExpression)
        ) {
            resultList.add(parseExpression(new Document(), rightExpression, null));
            resultList.add(parseExpression(new Document(), leftExpression, null));
            return resultList;
        }

        return Collections.emptyList();
    }

    private boolean isLogicalExpression(Expression expression) {
        return expression instanceof AndExpression || expression instanceof OrExpression;
    }
}
