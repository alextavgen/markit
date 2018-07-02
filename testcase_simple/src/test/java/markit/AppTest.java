package markit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import markit.exceptions.NotValidStateException;
import markit.models.PriceData;
import markit.services.PriceService;
import markit.services.PriceServiceImpl;
import markit.services.batchservice.BatchConsumer;
import markit.services.batchservice.BatchConsumerImpl;
import markit.storage.InMemoryPriceDataStorage;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedList;
import java.util.List;

/**
 * Main test
 */
public class AppTest
{

    private List<PriceData> makeTestPriceDataList(int begin, int end, int month, int price) {
        LocalDate date = LocalDate.of(2018, month, 28);
        List<PriceData> ret = new LinkedList<>();
        for (int i=begin; i< end; i++) {
            ret.add(new PriceData(String.valueOf(i), LocalDateTime.of(date, LocalTime.of(10, i, 0, 0)),
                    price));
        }
        return ret;
    }

    /**
     * Main Test
     */
    @Test
    public void requirementsTestForBlockingService()
    {

        BatchConsumer batchConsumer = new BatchConsumerImpl(new InMemoryPriceDataStorage());
        PriceService priceService = new PriceServiceImpl(batchConsumer);
        try {
            int initialPrice = 100;
            List<PriceData> priceDataList = makeTestPriceDataList(0, 10, 6, initialPrice);
            priceService.startBatch();
            priceService.uploadBatch(priceDataList);
            priceService.completeBatch();

            for (int i = 0; i < 10; i ++){
                assertEquals(initialPrice, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
            }

            int newPrice = 200;

            List<PriceData> priceDataNewList = makeTestPriceDataList(0, 7, 8, newPrice);

            priceService.startBatch();
            priceService.uploadBatch(priceDataNewList);

            // Test not completed batch
            for (int i = 0; i < 10; i ++){
                assertEquals(initialPrice, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
            }

            priceService.completeBatch();

            for (int i = 0; i < 7; i ++){
                assertEquals(newPrice, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
            }

            for (int i = 7; i < 10; i ++){
                assertEquals(initialPrice, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
            }

        } catch (NotValidStateException e) {

        }
    }
}
