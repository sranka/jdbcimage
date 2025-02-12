package io.github.sranka.jdbcimage;

import java.util.function.Consumer;

/**
 * Called in order to process results using generic instances provided. The consumer
 * <code>accept</code> method is called to process every row in the result set.
 */
public interface ResultConsumer<T> extends Consumer<T> {
    /**
     * Called to process a single row of the result.
     *
     * @param t object to get row data from
     */
    @Override
    void accept(T t);

    /**
     * Called before the result set rows are processed.
     *
     * @param info result set info
     */
    default void onStart(ResultSetInfo info) {
    }

    /**
     * Called upon finish of the processing.
     *
     * @return number of processed rows, -1 if unknown
     */
    default long onFinish() {
        return -1;
    }

    /**
     * Called upon processing failure.
     *
     * @param e exception
     */
    default void onFailure(Exception e) {
    }
}
