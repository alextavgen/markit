package markit.services.batchservice;
import markit.exceptions.NotValidStateException;
import markit.models.PriceData;
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


public class BatchConsumerTest {

    private List<PriceData> priceDataList;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private PriceDataStorage storage;

    @Before
    public void init(){

        storage = new InMemoryPriceDataStorage();

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
    public void testBatchConsumerBatchUploadOneTime() {
        BatchConsumer batchConsumer = new BatchConsumerImpl(storage);
        try {
            batchConsumer.startBatchProcessing();
            batchConsumer.uploadBatch(priceDataList);
            batchConsumer.completeBatchProcessing();
            for (int i = 0; i < 5; i ++){
                assertEquals(i, storage.getPriceDataById(String.valueOf(i)).getPayload());
            }

        }
        catch (NotValidStateException e) {

        }
    }

    @Test
    public void testBatchConsumerBatchUploadThreeTime() {
        BatchConsumer batchConsumer = new BatchConsumerImpl(storage);
        try {
            batchConsumer.startBatchProcessing();
            for (int i = 0; i < 3; i++) {
                batchConsumer.uploadBatch(priceDataList);
            }
            batchConsumer.completeBatchProcessing();
            for (int i = 0; i < 5; i ++){
                assertEquals(i, storage.getPriceDataById(String.valueOf(i)).getPayload());
            }

        }
        catch (NotValidStateException e) {

        }

    }

    @Test
    public void testBatchConsumerWrongBatchStart() throws Exception{

        exception.expect(NotValidStateException.class);
        exception.expectMessage("Not a Valid State for the Upload operation");

        BatchConsumer batchConsumer = new BatchConsumerImpl(storage);
        batchConsumer.uploadBatch(priceDataList);
        batchConsumer.completeBatchProcessing();
    }

    @Test
    public void testBatchConsumerWrongBatchUpload() throws Exception{
        exception.expect(NotValidStateException.class);
        exception.expectMessage("Not a Valid State for the Complete operation");

        BatchConsumer batchConsumer = new BatchConsumerImpl(storage);
        batchConsumer.completeBatchProcessing();
        batchConsumer.uploadBatch(priceDataList);

    }

    @Test
    public void testBatchUploadBeforeCancel() throws Exception{
        exception.expect(NotValidStateException.class);
        exception.expectMessage("Not a Valid State for the Start Batch operation");

        BatchConsumer batchConsumer = new BatchConsumerImpl(storage);
        batchConsumer.startBatchProcessing();
        batchConsumer.uploadBatch(priceDataList);
        batchConsumer.startBatchProcessing();
        batchConsumer.uploadBatch(priceDataList);

    }

}
