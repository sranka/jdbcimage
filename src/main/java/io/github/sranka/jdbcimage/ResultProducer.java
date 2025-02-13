package io.github.sranka.jdbcimage;

/**
 * Pull parser that is called in order to obtain row data.
 */
public interface ResultProducer {
    /**
     * Called to initialize the provider.
     *
     * @return initialized row data
     */
    RowData start();

    /**
     * Invoked in order to fill the data supplied.
     *
     * @param row row to fill in
     * @return false if no data can be filled
     */
    boolean fillData(RowData row);

    /**
     * Called to inform about finished processing.
     */
    @SuppressWarnings("EmptyMethod")
    void close();
}
