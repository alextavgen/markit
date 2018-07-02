package markit.services;

import markit.exceptions.NotValidStateException;
import markit.models.PriceData;
import markit.services.batchservice.BatchConsumer;
import markit.storage.InMemoryPriceDataStorage;
import markit.storage.PriceDataStorage;
import org.apache.log4j.Logger;

import java.util.List;

public class PriceServiceImpl implements PriceService {

    final static Logger logger = Logger.getLogger(PriceServiceImpl.class);

    private final PriceDataStorage priceDataStorage;

    private BatchConsumer batchConsumer;

    public PriceServiceImpl(BatchConsumer batchConsumer){
        this.batchConsumer = batchConsumer;
        this.priceDataStorage = new InMemoryPriceDataStorage();
    }

    @Override
    public void startBatch() throws NotValidStateException{
        logger.info("Starting batch processing");
        this.batchConsumer.startBatchProcessing();
    }

    @Override
    public void uploadBatch(List<PriceData> priceDataList) throws NotValidStateException {
        logger.info("Starting uploading bulkdata with size: " + priceDataList.size());
        batchConsumer.uploadBatch(priceDataList);
    }

    @Override
    public void completeBatch() throws NotValidStateException {
        logger.info("Completed batch processing");
        batchConsumer.completeBatchProcessing();
        synchronized(this) {
            priceDataStorage.mergeStorages(batchConsumer.getBatchPriceDataStorage());
        }
    }

    @Override
    public void cancelBatch() throws NotValidStateException {
        logger.info("Canceled batch processing");
        batchConsumer.cancelBatchProcessing();
    }

    @Override
    public PriceData getLatestPriceDataById(String id) {
        return priceDataStorage.getPriceDataById(id);
    }
}
