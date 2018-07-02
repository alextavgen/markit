package markit.storage;

import markit.models.PriceData;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Interface which abstracts Price data storage
 * @author aleksandr tavgen
 */

public interface PriceDataStorage {

    void init();

    ConcurrentMap<String, PriceData> getStorageAsMap();

    void mergeStorages(PriceDataStorage priceDataStorage);

    PriceData getPriceDataById( String id);
}
