package markit.storage;

import markit.models.PriceData;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class PriceDataStorageTest {

    private List<PriceData> priceDataList;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void init(){
        priceDataList = makeTestPriceDataList(0, 5);
    }

    private List<PriceData> makeTestPriceDataList(int begin, int end) {
        LocalDate date = LocalDate.of(2018, 06, 28);
        List<PriceData> ret = new LinkedList<>();
        for (int i=begin; i< end; i++) {
            ret.add(new PriceData(String.valueOf(i), LocalDateTime.of(date, LocalTime.of(10, i, 0, 0)),
                    i));
        }
        return ret;
    }


    @Test
    public void setPriceDataTest(){
        PriceDataStorage storage = new InMemoryPriceDataStorage();
        LocalDate date = LocalDate.of(2018, 06, 28);

        PriceData priceDataUnique = new PriceData("1", LocalDateTime.of(date, LocalTime.of(10, 1, 0, 0)),
                "Unique Value");
        PriceData priceDataSameOlder = new PriceData("2", LocalDateTime.of(date, LocalTime.of(10, 2, 0, 0)),
                "Non Unique Old Value");
        PriceData priceDataSameNewer = new PriceData("2", LocalDateTime.of(date, LocalTime.of(10, 3, 0, 0)),
                "Non Unique New Value");

        storage.setPriceData(priceDataUnique);
        storage.setPriceData(priceDataSameOlder);
        storage.setPriceData(priceDataSameNewer);

        assertEquals("Non Unique New Value", storage.getPriceDataById("2").getPayload());
        assertEquals("Unique Value", storage.getPriceDataById("1").getPayload());

    }

    @Test
    public void setPriceDataListTest(){
        PriceDataStorage storage = new InMemoryPriceDataStorage();
        storage.setPriceDataAsList(priceDataList);
        assertEquals(5, storage.getStorageMap().size());

        for (int i = 0; i < 5; i ++){
            assertEquals(i, storage.getPriceDataById(String.valueOf(i)).getPayload());
        }

    }

    // Merge with update on newer sites
    @Test
    public void mergePriceDataListTest(){
        PriceDataStorage storageOne = new InMemoryPriceDataStorage();
        PriceDataStorage storageTwo = new InMemoryPriceDataStorage();
        storageOne.setPriceDataAsList(priceDataList);

        LocalDate date = LocalDate.of(2018, 06, 28);
        PriceData priceDataSameIdNewer = new PriceData("1", LocalDateTime.of(date, LocalTime.of(10, 10, 0, 0)),
                "New Value for 1");

        PriceData priceDataSameNewer2 = new PriceData("3", LocalDateTime.of(date, LocalTime.of(10, 8, 0, 0)),
                "New Value for 3");

        PriceData priceDataUnique = new PriceData("5", LocalDateTime.of(date, LocalTime.of(10, 2, 0, 0)),
                "Test");
        PriceData priceDataUnique2 = new PriceData("6", LocalDateTime.of(date, LocalTime.of(10, 3, 0, 0)),
                "Test");

        storageTwo.setPriceData(priceDataSameIdNewer);
        storageTwo.setPriceData(priceDataUnique);
        storageTwo.setPriceData(priceDataUnique2);
        storageTwo.setPriceData(priceDataSameNewer2);


        storageOne.mergeStorages(storageTwo);

        assertEquals(7, storageOne.getStorageMap().size());
        assertEquals("New Value for 1", storageOne.getPriceDataById("1").getPayload());
        assertEquals("New Value for 3", storageOne.getPriceDataById("3").getPayload());
    }

}
