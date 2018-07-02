package markit.services;

import markit.exceptions.NotValidStateException;
import markit.models.PriceData;
import markit.services.batchservice.BatchConsumer;
import markit.storage.PriceDataStorage;
import org.apache.log4j.Logger;

import java.util.List;

public class PriceServiceImpl implements PriceService {

    final static Logger logger = Logger.getLogger(PriceServiceImpl.class);

    private final PriceDataStorage priceDataStorage;

    private BatchConsumer batchConsumer;

    public PriceServiceImpl(PriceDataStorage storage, BatchConsumer batchConsumer){
        this.priceDataStorage = storage;
        this.batchConsumer = batchConsumer;
    }

    @Override
    public void startBatch() throws NotValidStateException{
        logger.debug("Starting batch processing");
        this.batchConsumer.startBatchProcessing();
    }

    @Override
    public void uploadBatch(List<PriceData> priceDataList) throws NotValidStateException {
        logger.debug("Starting upload bulk data with size: " + priceDataList.size());
        batchConsumer.uploadBatch(priceDataList);
    }

    @Override
    public void completeBatch() throws NotValidStateException {
        logger.debug("Completed batch processing");
        batchConsumer.completeBatchProcessing();
        priceDataStorage.mergeStorages(batchConsumer.getBatchPriceDataStorage());
    }

    @Override
    public void cancelBatch() throws NotValidStateException {
        logger.debug("Canceled batch processing");
        batchConsumer.cancelBatchProcessing();
    }

    @Override
    public PriceData getLatestPriceDataById(String id) {
        return priceDataStorage.getPriceDataById(id);
    }
}
