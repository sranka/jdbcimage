package pz.tool.jdbcimage;

import java.time.Duration;

/**
 * Transfers data from producer to consumer.
 * @author zavora
 */
public class ResultProducerRunner {
	private ResultProducer producer;
	private ResultConsumer<RowData> consumer;

	public ResultProducerRunner(ResultProducer producer, ResultConsumer<RowData> consumer) {
		super();
		this.producer = producer;
		this.consumer = consumer;
	}

	public void run(){
		started = System.currentTimeMillis();
		try{
			// read header (version)
			RowData token = producer.start();
			consumer.onStart(token.info);
			while(producer.fillData(token)){
				consumer.accept(token);
			}
			// read item
			consumer.onFinish();
		} catch (Exception e){
			consumer.onFailure(e);
			throw new RuntimeException(e);
		} finally{
			producer.close();
			finished = System.currentTimeMillis();
		}
	}

	// time stamps
	public long started;
	public long finished;

	public Duration getDuration(){
		return Duration.ofMillis(finished - started);
	}
}
