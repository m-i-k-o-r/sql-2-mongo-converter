package com.koroli.queryconverter.operators.object;

import com.koroli.queryconverter.exceptions.QueryConversionException;
import com.koroli.queryconverter.operators.MongoOperator;
import com.koroli.queryconverter.operators.object.comparisons.EqualsComparison;
import com.koroli.queryconverter.operators.object.comparisons.InComparison;
import com.koroli.queryconverter.operators.object.comparisons.NotEqualsComparison;
import lombok.Builder;
import lombok.Value;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import org.bson.Document;

/**
 * Represents an operator for object-based comparisons in MongoDB.
 */
@Value
@Builder
public class ObjectOperator implements MongoOperator {
    Object value;
    String column;
    Expression comparisonExpression;

    /**
     * Converts this operator into a MongoDB-compatible {@link Document}.
     *
     * @return the MongoDB document representing the operator.
     * @throws QueryConversionException if the comparison type is unsupported.
     */
    @Override
    public Document toDocument() throws QueryConversionException {
        ObjectComparison comparison = switch (comparisonExpression) {
            case EqualsTo ignored            -> new EqualsComparison();
            case NotEqualsTo ignored         -> new NotEqualsComparison();
            case InExpression inExpression   -> new InComparison(inExpression.isNot());
            default -> throw new QueryConversionException("Unsupported comparison expression: " + comparisonExpression);
        };

        return comparison.compare(value);
    }
}
