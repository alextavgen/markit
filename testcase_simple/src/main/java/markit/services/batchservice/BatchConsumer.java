package markit.services.batchservice;

import markit.exceptions.NotValidStateException;
import markit.models.PriceData;
import markit.storage.PriceDataStorage;

import java.util.List;

/**
 * Interface for BatchUpload Functionality
 * @author aleksandr tavgen
 */

public interface BatchConsumer{
    void startBatchProcessing() throws NotValidStateException;
    void uploadBatch (List<PriceData> priceDataList) throws NotValidStateException;
    void cancelBatchProcessing () throws NotValidStateException;
    void completeBatchProcessing () throws NotValidStateException;
    PriceDataStorage getBatchPriceDataStorage() throws NotValidStateException;

}
