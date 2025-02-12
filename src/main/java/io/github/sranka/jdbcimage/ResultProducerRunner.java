package io.github.sranka.jdbcimage;

/**
 * Transfers data from producer to consumer.
 * @author zavora
 */
public class ResultProducerRunner {
	private final ResultProducer producer;
	private final ResultConsumer<RowData> consumer;

	public ResultProducerRunner(ResultProducer producer, ResultConsumer<RowData> consumer) {
		super();
		this.producer = producer;
		this.consumer = consumer;
	}

	public long run(){
		try{
			// read header (version)
			RowData token = producer.start();
			consumer.onStart(token.info);
			while(producer.fillData(token)){
				consumer.accept(token);
			}
			// read item
			return consumer.onFinish();
		} catch (Exception e){
			consumer.onFailure(e);
			throw new RuntimeException(e);
		} finally{
			producer.close();
		}
	}
}
