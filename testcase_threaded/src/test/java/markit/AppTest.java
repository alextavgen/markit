package markit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import markit.exceptions.NotValidStateException;
import markit.models.PriceData;
import markit.services.PriceService;
import markit.services.PriceServiceImpl;
import markit.services.batchservice.BatchConsumer;
import markit.services.batchservice.BatchConsumerImpl;

import markit.storage.InMemoryPriceDataStorage;
import markit.storage.PriceDataStorage;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedList;
import java.util.List;


class Producer implements Runnable{

    final static Logger logger = Logger.getLogger(Producer.class);

    private final PriceService priceService;

    private final int batchsize;

    private LocalDateTime localDateTime;

    private int price;

    public Producer (PriceService priceService, int batchsize, int price, LocalDateTime localDateTime){
        this.priceService = priceService;
        this.batchsize = batchsize;
        this.localDateTime = localDateTime;
        this.price = price;
    }


    public void run() {
        try {
            priceService.startBatch();
            for (int i = 0; i < batchsize; i++) {
                List<PriceData> priceDataList = new LinkedList<>();
                for (int j = 0; j < 1000; j++){
                    priceDataList.add (new PriceData(String.valueOf(i * 1000 + j),
                            localDateTime,
                            price));
                }
                priceService.uploadBatch(priceDataList);


                logger.debug("Producer ID "+Thread.currentThread().getId()+" uploaded batch nr: "+i);
            }
            priceService.completeBatch();

        } catch (NotValidStateException e) {
            e.printStackTrace();
        }
    }
}

class ProducerBatchCompleted implements Runnable{

    final static Logger logger = Logger.getLogger(Producer.class);

    private final PriceService priceService;


    public ProducerBatchCompleted (PriceService priceService){
        this.priceService = priceService;
    }


    public void run() {
        try {
            priceService.completeBatch();

        } catch (NotValidStateException e) {
            e.printStackTrace();
        }
    }
}


class Consumer implements Runnable {

    private final int batchSize;

    private final PriceService priceService;

    public Consumer (PriceService priceService, int batchSize){
        this.priceService = priceService;
        this.batchSize = batchSize;
    }

    final static Logger logger = Logger.getLogger(Consumer.class);

    public void run() {
        for (int i = 0; i < batchSize; i++){
            logger.debug("Got PriceData by id " + i + " ----- " + priceService.getLatestPriceDataById(String.valueOf(i)));
        }

    }
}
/**
 * Unit test for simple App.
 */
public class AppTest
{
    final static Logger logger = Logger.getLogger(AppTest.class);
    /**
     * Main flow test
     */
    @Test
    public void testSequence(){
        seqTest();
        concurrentTest();
    }

    @Test
    public void runProducingConsuming() throws InterruptedException {
        PriceDataStorage storage = new InMemoryPriceDataStorage();
        PriceDataStorage batchStorage = new InMemoryPriceDataStorage();


        BatchConsumer consumer = new BatchConsumerImpl(batchStorage, 8);
        PriceService priceService = new PriceServiceImpl(storage,consumer);

        LocalDate date = LocalDate.of(2018, 6, 28);

        LocalDateTime localDateTime = LocalDateTime.of(date, LocalTime.of(10, 15, 0, 0));

        Producer producer = new Producer(priceService, 10, 100, localDateTime);

        LocalDateTime localDateTimeNew = LocalDateTime.of(date, LocalTime.of(12, 15, 0, 0));

        Producer producerUpdater = new Producer(priceService, 5, 200, localDateTimeNew);

        Thread produce = new Thread(producer);

        producer.run();

        Thread produceUpdater = new Thread(producerUpdater);
        produce.start();
        for (int i = 0; i < 10000; i++){
            assertEquals(100, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
        }
        produce.join();
        produceUpdater.start();
        produceUpdater.join();
        for (int i = 0; i < 5000; i++){
            assertEquals(200, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
        }
        for (int i = 5000; i < 10000; i++){
            assertEquals(100, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
        }

    }

    @Test
    public void runConcurrentProducingConsuming() throws InterruptedException {
        PriceDataStorage storage = new InMemoryPriceDataStorage();
        PriceDataStorage batchStorage = new InMemoryPriceDataStorage();


        BatchConsumer consumer = new BatchConsumerImpl(batchStorage, 8);
        PriceService priceService = new PriceServiceImpl(storage,consumer);

        LocalDate date = LocalDate.of(2018, 6, 28);

        LocalDateTime localDateTime = LocalDateTime.of(date, LocalTime.of(10, 20, 0, 0));

        Producer producer = new Producer(priceService, 100, 100, localDateTime);

        LocalDateTime localDateTimeNew = LocalDateTime.of(date, LocalTime.of(12, 15, 0, 0));

        Producer producerUpdater = new Producer(priceService, 5, 200, localDateTimeNew);

        Thread produce = new Thread(producer);

        Thread produceUpdater = new Thread(producerUpdater);
        produce.start();

        produce.join();
        produceUpdater.start();
        produceUpdater.join();
        for (int i = 0; i < 5000; i++){
            assertEquals(200, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
        }
        for (int i = 5000; i < 10000; i++){
            assertEquals(100, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
        }

    }

    @Test
    public void testConcurrentBatchCompletedConsumer() throws NotValidStateException {
        int nrOfBatches = 1_000;

        PriceDataStorage storage = new InMemoryPriceDataStorage();
        PriceDataStorage batchStorage = new InMemoryPriceDataStorage();


        BatchConsumer consumer = new BatchConsumerImpl(batchStorage, 8);
        PriceService priceService = new PriceServiceImpl(storage,consumer);
        LocalDate date = LocalDate.of(2018, 6, 28);

        LocalDateTime localDateTime = LocalDateTime.of(date, LocalTime.of(10, 20, 0, 0));

        Producer producer = new Producer(priceService, nrOfBatches, 100, localDateTime);

        Thread produce = new Thread(producer);

        produce.start();
        try {
            produce.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    public void concurrentTest(){
        int nrOfBatches = 1_000;

        PriceDataStorage storage = new InMemoryPriceDataStorage();
        PriceDataStorage batchStorage = new InMemoryPriceDataStorage();


        BatchConsumer consumer = new BatchConsumerImpl(batchStorage, 8);
        PriceService priceService = new PriceServiceImpl(storage,consumer);

        LocalDate date = LocalDate.of(2018, 6, 28);

        LocalDateTime localDateTime = LocalDateTime.of(date, LocalTime.of(10, 20, 0, 0));
        LocalDateTime localDateTimeNew = LocalDateTime.of(date, LocalTime.of(12, 15, 0, 0));


        try {
            priceService.startBatch();


            for (int i = 0; i < nrOfBatches; i++) {
                List<PriceData> priceDataList = new LinkedList<>();
                for (int j = 0; j < 1000; j++) {
                    priceDataList.add(new PriceData(String.valueOf(i * 1000 + j),
                            localDateTimeNew,
                            250));
                }
                priceService.uploadBatch(priceDataList);
            }
        } catch (NotValidStateException e) {
            e.printStackTrace();
        }
        Consumer idConsumer = new Consumer(priceService, nrOfBatches);

        ProducerBatchCompleted completeBatchInThread = new ProducerBatchCompleted(priceService);

        long startTime = System.currentTimeMillis();

        Thread threadedConsumer = new Thread(idConsumer);
        Thread threadedBatchCompleted = new Thread(completeBatchInThread);
        threadedBatchCompleted.start();

        threadedConsumer.start();


        try {
            threadedBatchCompleted.join();
            threadedConsumer.join();
            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            logger.info("Concurrent execution time " + elapsedTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    public void seqTest(){
        int nrOfBatches = 1_000;

        PriceDataStorage storage = new InMemoryPriceDataStorage();
        PriceDataStorage batchStorage = new InMemoryPriceDataStorage();


        BatchConsumer consumer = new BatchConsumerImpl(batchStorage, 8);
        PriceService priceService = new PriceServiceImpl(storage,consumer);

        LocalDate date = LocalDate.of(2018, 6, 28);
        
        LocalDateTime localDateTimeNew = LocalDateTime.of(date, LocalTime.of(12, 15, 0, 0));


        try {
            priceService.startBatch();


            for (int i = 0; i < nrOfBatches; i++) {
                List<PriceData> priceDataList = new LinkedList<>();
                for (int j = 0; j < 1000; j++) {
                    priceDataList.add(new PriceData(String.valueOf(i * 1000 + j),
                            localDateTimeNew,
                            100));
                }
                priceService.uploadBatch(priceDataList);
            }
        } catch (NotValidStateException e) {
            e.printStackTrace();
        }

        Consumer idConsumer = new Consumer(priceService, nrOfBatches);

        ProducerBatchCompleted completeBatchInThread = new ProducerBatchCompleted(priceService);



        Thread threadedConsumer = new Thread(idConsumer);
        Thread threadedBatchCompleted = new Thread(completeBatchInThread);
        try {

            threadedBatchCompleted.start();
            threadedBatchCompleted.join();

            long startTime = System.currentTimeMillis();

            threadedConsumer.start();
            threadedConsumer.join();

            long stopTime = System.currentTimeMillis();

            long elapsedTime = stopTime - startTime;
            logger.info("Sequential execution time " + elapsedTime);



        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
