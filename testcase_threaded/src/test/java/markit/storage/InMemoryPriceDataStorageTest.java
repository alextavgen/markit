package markit.storage;

import markit.models.PriceData;

import org.junit.Before;
import org.junit.Test;


import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

public class InMemoryPriceDataStorageTest {

    private ConcurrentHashMap<String, PriceData> testMap;

    @Before
    public void init(){
        testMap = makeTestMap(0, 10, 6, 100);
    }

    public ConcurrentHashMap<String, PriceData> makeTestMap(int begin, int end, int month, int priceDataValue) {
        LocalDate date = LocalDate.of(2018, month, 28);
        ConcurrentHashMap<String, PriceData> result = new ConcurrentHashMap<>();
        for (int i=begin; i< end; i++) {
            String id = String.valueOf(i);
            result.put(id, new PriceData(id, LocalDateTime.of(date, LocalTime.of(10, i, 0, 0)),
                    priceDataValue));
        }
        return result;
    }

    @Test
    public void getPriceDataByIdTest(){
        PriceDataStorage storage = new InMemoryPriceDataStorage(testMap);

        assertEquals(100, storage.getPriceDataById("8").getPayload());
    }

    // Merge with update on newer sites
    @Test
    public void mergePriceDataListTest(){
        PriceDataStorage storage = new InMemoryPriceDataStorage(testMap);

        ConcurrentHashMap<String, PriceData> mergingMap = makeTestMap(0, 7,8, 200);

        PriceDataStorage storageForMerging = new InMemoryPriceDataStorage(mergingMap);

        storage.mergeStorages(storageForMerging);

        assertEquals(10, storage.getStorageAsMap().size());
        assertEquals(7, storageForMerging.getStorageAsMap().size());

        assertEquals(200, storage.getPriceDataById("1").getPayload());
        assertEquals(100, storage.getPriceDataById("8").getPayload());
    }

}
