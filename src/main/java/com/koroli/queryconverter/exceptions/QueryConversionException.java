package com.koroli.queryconverter.exceptions;

/**
 * Exception that is thrown when there is an issue parsing a sql statement.
 */
// almost unchanged code from the "original" code for traversing the nodes...
public class QueryConversionException extends Exception {
    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message saved for later retrieval by {@link #getMessage()}.
     */
    public QueryConversionException(final String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message saved for later retrieval by {@link #getMessage()}.
     * @param cause   the cause saved for later retrieval by {@link #getCause()}.
     */
    public QueryConversionException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the specified cause.
     *
     * @param cause the cause of this exception.
     */
    public QueryConversionException(final Throwable cause) {
        super(fixErrorMessage(cause));
    }

    private static Throwable fixErrorMessage(final Throwable cause) {
        if (cause.getMessage().startsWith("Encountered unexpected token: \"=\" \"=\"")) {
            return new QueryConversionException(
                    "Unable to parse complete SQL string. One reason for this is the use of double equals (==).", cause);
        }
        if (cause.getMessage().startsWith("Encountered \" \"(\" \"( \"\"")) {
            return new QueryConversionException(
                    "Only one simple table name is supported.", cause);
        }
        if (cause.getMessage().contains("Was expecting:" + System.lineSeparator() + "    \"SELECT\"")) {
            return new QueryConversionException(
                    "Only SELECT statements are supported.", cause);
        }

        return cause;
    }
}
