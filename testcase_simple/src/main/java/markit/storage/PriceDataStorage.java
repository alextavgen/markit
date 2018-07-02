package markit.storage;

import markit.models.PriceData;

import java.util.List;
import java.util.Map;

/**
 * Interface which abstracts Price data storage
 * @author aleksandr tavgen
 */

public interface PriceDataStorage {

    void init();

    void setPriceData (PriceData priceData);

    PriceData getPriceDataById( String id);

    void setPriceDataAsList(List<PriceData> priceDataList);

    void mergeStorages(PriceDataStorage priceDataStorage);

    Map<String, PriceData> getStorageMap();
}
