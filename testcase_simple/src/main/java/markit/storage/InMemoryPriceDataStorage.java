package markit.storage;

import markit.models.PriceData;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * In Memory implementation of Price data storage
 * @author aleksandr tavgen
 */

public class InMemoryPriceDataStorage implements PriceDataStorage{

    private static final int ESTIMATED_BATCH = 1_000_000;

    private final int requiredCapacity;

    private Map<String, PriceData> storage;

    /**
     *  Lambda which compares two PriceData and returns with a latest time
     */
    private final BiFunction<PriceData, PriceData, PriceData> comparingPriceDataBiFunction =
            (oldValue, newValue) -> newValue.getAsOf().compareTo(oldValue.getAsOf()) >= 0 ? newValue: oldValue;

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

    /**
     * Set Price Data in the Storage only if PriceData is newer that existing if exists
     */
    @Override
    public void setPriceData(PriceData priceData) {
        if (storage.get(priceData) == null){
            storage.put(priceData.getId(), priceData);
        }
        else {
            storage.computeIfPresent(priceData.getId(),
                    (k, oldValue) -> priceData.getAsOf().compareTo(oldValue.getAsOf()) >= 0 ? priceData : oldValue);
        }
    }

    @Override
    public PriceData getPriceDataById(String id) {
        return this.storage.get(id);
    }

    /**
     *  Bulk load to Price Data Storage of PriceData List
     */
    @Override
    public void setPriceDataAsList(List<PriceData> priceDataList) {
        Map<String, PriceData> batchMap =  priceDataList.stream().collect(Collectors.toMap(PriceData::getId,
                        Function.identity(),
                (oldValue, newValue) -> newValue.getAsOf().compareTo(oldValue.getAsOf()) >= 0 ? newValue: oldValue));

        mergeStorageMaps(batchMap);
    }

    /**
     *  Merges two PriceData Storages, values with newer datetime are set
     */
    private void mergeStorageMaps(Map<String, PriceData> batchMap) {
        batchMap.forEach((k, v) -> this.storage.merge(k, v,
                comparingPriceDataBiFunction));
    }

    @Override
    public void mergeStorages(PriceDataStorage priceDataStorage) {
        mergeStorageMaps(priceDataStorage.getStorageMap());
    }

    @Override
    public Map<String, PriceData> getStorageMap() {
        return this.storage;
    }
}
