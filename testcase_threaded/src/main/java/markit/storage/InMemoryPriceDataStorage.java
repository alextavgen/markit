package markit.storage;

import markit.models.PriceData;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/**
 * In Memory implementation of Price data storage
 * @author aleksandr tavgen
 */

public class InMemoryPriceDataStorage implements PriceDataStorage{

    final static Logger logger = Logger.getLogger(InMemoryPriceDataStorage.class);

    private static final int ESTIMATED_BATCH = 1_000_000;

    private final int requiredCapacity;

    private ConcurrentMap<String, PriceData> storage;

    /**
     *  Lambda which compares two PriceData and returns with a latest time
     */
    private final BiFunction<PriceData, PriceData, PriceData> comparingPriceDataBiFunction =
            (oldValue, newValue) -> newValue.getAsOf().compareTo(oldValue.getAsOf()) >= 0 ? newValue: oldValue;

    public InMemoryPriceDataStorage(ConcurrentMap<String, PriceData> priceDataMap) {
        this.storage = priceDataMap;
        this.requiredCapacity = ESTIMATED_BATCH;
    }

    public InMemoryPriceDataStorage() {
        this(ESTIMATED_BATCH);
    }

    public InMemoryPriceDataStorage(int requiredCapacity) {
        this.requiredCapacity = requiredCapacity;
        this.init();
    }

    /**
     * Reinitialize data storage
     *
     */
    @Override
    public void init() {
        this.storage = new ConcurrentHashMap<>((int) Math.ceil(this.requiredCapacity / 0.75));
    }

    @Override
    public ConcurrentMap<String, PriceData> getStorageAsMap(){
        return storage;
    }

    /**
     *  Merges two PriceData Storages, values with newer datetime are set
     */
    private void mergeStorageMaps(ConcurrentMap<String, PriceData> batchMap) {
        logger.debug("Merging map with size: " + batchMap.size() + " with internal storage size: " + this.storage.size());
        batchMap.forEach((k, v) -> this.storage.merge(k, v,
                comparingPriceDataBiFunction));
    }

    @Override
    public void mergeStorages(PriceDataStorage priceDataStorage) {
        mergeStorageMaps(priceDataStorage.getStorageAsMap());
    }

    @Override
    public PriceData getPriceDataById(String id) {
        return this.storage.get(id);
    }
}
