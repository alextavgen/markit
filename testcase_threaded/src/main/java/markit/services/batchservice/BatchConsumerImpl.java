package markit.services.batchservice;

import markit.exceptions.NotValidStateException;
import markit.models.PriceData;
import markit.storage.PriceDataStorage;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;


public class BatchConsumerImpl implements BatchConsumer {

    final static Logger logger = Logger.getLogger(BatchConsumerImpl.class);

    private BatchRunState state;

    private ExecutorService executorService;

    private final int nThreads;

    private final PriceDataStorage priceStorage;

    private final List<Future<PriceDataStorage>> resultList;

    public BatchConsumerImpl(PriceDataStorage storage, int nThreads){
        this.priceStorage = storage;
        this.state = BatchRunState.INITIALISATION;
        this.nThreads = nThreads;
        this.resultList = new LinkedList<>();
    }

    /**
     *  Start Batch processing check for current state, start Executors for threaded service
     */
    @Override
    public void startBatchProcessing() throws NotValidStateException{
        if (this.state == BatchRunState.INITIALISATION || this.state == BatchRunState.CANCELED
                || this.state == BatchRunState.COMPLETED){

            this.executorService =  Executors.newFixedThreadPool(nThreads);

            this.state = BatchRunState.STARTED;
        }
        else {
            logger.error("Not a valid state for batch operation from " + this.state + " to " + BatchRunState.STARTED);
            throw new NotValidStateException( "Not a Valid State for the Start Batch operation");
        }

    }

    /**
     *  Bulk load submitted as a Future to the Executor Service
     */
    @Override
    public void uploadBatch(List<PriceData> priceDataList) throws NotValidStateException {
        if (this.state == BatchRunState.STARTED) {
            this.state = BatchRunState.UPLOAD;
        }

        if (this.state == BatchRunState.UPLOAD) {
            // put in the queue
            for (int i = 0; i < this.nThreads; i++){
                resultList.add(executorService.submit(new CallableConsumer(priceDataList)));
                logger.debug("Started Threaded Consumer number " + i);
            }

        }
        else {
            logger.error("Not a valid state for batch operation from " + this.state + " to " + BatchRunState.UPLOAD);
            throw new NotValidStateException( "Not a Valid State for the Upload operation");
        }

    }
    /**
     *  Completed Batch Upload, get data from Futures List and merging with main data storage
     */
    @Override
    public void completeBatchProcessing() throws NotValidStateException{
        if (this.state ==BatchRunState.UPLOAD){
            for (Future<PriceDataStorage> priceDataStorageFuture : resultList){
                try {
                    this.priceStorage.mergeStorages(priceDataStorageFuture.get());
                    logger.debug("Merged, storage size: " + priceStorage.getStorageAsMap().size());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            this.state = BatchRunState.COMPLETED;


        }
        else {
            logger.error("Not a valid state for batch operation from " + this.state + " to " + BatchRunState.COMPLETED);
            throw new NotValidStateException( "Not a Valid State for the Complete operation");
        }

    }


    @Override
    public void cancelBatchProcessing() throws NotValidStateException{
        if (this.state ==BatchRunState.UPLOAD){
            this.state = BatchRunState.CANCELED;
            executorService.shutdownNow();
            this.priceStorage.init();
        }
        else {
            logger.error("Not a valid state for batch operation from " + this.state + " to " + BatchRunState.CANCELED);
            throw new NotValidStateException( "Not a Valid State for the Cancel operation");
        }

    }
    /**
     *  Get batch Price DataStorage
     */
    @Override
    public PriceDataStorage getBatchPriceDataStorage() throws NotValidStateException{
        if (this.state == BatchRunState.COMPLETED){
            return this.priceStorage;
        }
        else {
            logger.error("Not a valid state for batch operation from " + this.state);
            throw new NotValidStateException("Not a Valid State for the ReturnBatchStorage operation");
        }
    }

}
