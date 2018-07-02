package markit.services;

import markit.exceptions.NotValidStateException;
import markit.models.PriceData;

import java.util.List;

/**
 * Interface for PriceService
 * @author aleksandr tavgen
 */

public interface PriceService {
    void startBatch() throws NotValidStateException;
    void uploadBatch (List<PriceData> priceDataList) throws NotValidStateException;
    void completeBatch() throws NotValidStateException;
    void cancelBatch() throws NotValidStateException;
    PriceData getLatestPriceDataById(String id);
}
