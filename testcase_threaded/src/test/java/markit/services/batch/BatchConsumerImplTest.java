package markit.services.batch;
import markit.exceptions.NotValidStateException;
import markit.models.PriceData;
import markit.services.batchservice.BatchConsumer;
import markit.services.batchservice.BatchConsumerImpl;
import markit.storage.InMemoryPriceDataStorage;
import markit.storage.PriceDataStorage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.junit.Assert.assertEquals;


import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedList;
import java.util.List;

public class BatchConsumerImplTest {

    private List<PriceData> priceDataList;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void init(){
        priceDataList = makeTestPriceDataList(0, 5);
    }

    public List<PriceData> makeTestPriceDataList(int begin, int end) {
        LocalDate date = LocalDate.of(2018, 06, 28);
        List<PriceData> ret = new LinkedList<>();
        for (int i=begin; i<=end; i++) {
            ret.add(new PriceData(String.valueOf(i), LocalDateTime.of(date, LocalTime.of(10, i, 0, 0)),
                    i));
        }
        return ret;
    }


    @Test
    public void testBatchConsumerBatchUploadThreeTime() {
        PriceDataStorage batchStorage = new InMemoryPriceDataStorage();

        BatchConsumer consumer = new BatchConsumerImpl(batchStorage, 1);
        try {
            consumer.startBatchProcessing();
            for (int i = 0; i < 3; i++) {
                consumer.uploadBatch(makeTestPriceDataList(0, i * 2));
            }
            consumer.completeBatchProcessing();

            for (int i = 0; i < 5; i ++){
                assertEquals(i, consumer.getBatchPriceDataStorage().getPriceDataById(String.valueOf(i)).getPayload());
            }

        }
        catch (NotValidStateException e) {
        }

    }

    @Test
    public void testBatchConsumerBatchUploadOneTime() {
        PriceDataStorage batchStorage = new InMemoryPriceDataStorage();

        BatchConsumer consumer = new BatchConsumerImpl(batchStorage, 1);
        try {
            consumer.startBatchProcessing();
            consumer.uploadBatch(makeTestPriceDataList(0, 10));

            consumer.completeBatchProcessing();

            for (int i = 0; i < 5; i ++){
                assertEquals(i, consumer.getBatchPriceDataStorage().getPriceDataById(String.valueOf(i)).getPayload());
            }

        }
        catch (NotValidStateException e) {
        }

    }
    @Test
    public void testBatchConsumerWrongBatchStart() throws Exception{

        exception.expect(NotValidStateException.class);
        exception.expectMessage("Not a Valid State for the Upload operation");

        PriceDataStorage batchStorage = new InMemoryPriceDataStorage();

        BatchConsumer consumer = new BatchConsumerImpl(batchStorage, 1);
        consumer.uploadBatch(priceDataList);
        consumer.completeBatchProcessing();
    }

    @Test
    public void testBatchConsumerWrongBatchUpload() throws Exception{
        exception.expect(NotValidStateException.class);
        exception.expectMessage("Not a Valid State for the Complete operation");

        PriceDataStorage batchStorage = new InMemoryPriceDataStorage();

        BatchConsumer consumer = new BatchConsumerImpl(batchStorage, 1);

        consumer.completeBatchProcessing();
        consumer.uploadBatch(priceDataList);

    }

    @Test
    public void testBatchUploadBeforeCancel() throws Exception{
        exception.expect(NotValidStateException.class);
        exception.expectMessage("Not a Valid State for the Start Batch operation");

        PriceDataStorage batchStorage = new InMemoryPriceDataStorage();

        BatchConsumer consumer = new BatchConsumerImpl(batchStorage, 1);

        consumer.startBatchProcessing();
        consumer.uploadBatch(priceDataList);
        consumer.startBatchProcessing();
        consumer.uploadBatch(priceDataList);

    }

}
