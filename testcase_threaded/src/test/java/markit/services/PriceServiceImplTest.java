package markit.services;

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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class PriceServiceImplTest {

    private List<PriceData> priceDataList;

    @Mock
    BatchConsumer batchConsumerMock;

    @Mock
    PriceDataStorage priceDataStorageMock;

    @Mock
    PriceDataStorage batchDataStorageMock;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void init(){
        priceDataList = makeTestPriceDataList(0, 5);
        MockitoAnnotations.initMocks(this);
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

    public List<PriceData> makeTestPriceDataWithNewMonthList(int begin, int end, int month) {
        LocalDate date = LocalDate.of(2018, month, 28);
        List<PriceData> ret = new LinkedList<>();
        for (int i=begin; i< end; i++) {
            ret.add(new PriceData(String.valueOf(i), LocalDateTime.of(date, LocalTime.of(10, i, 0, 0)),
                    i*2));
        }
        return ret;
    }

    @Test
    public void priceServiceSimpleTest() {
        PriceService priceService = new PriceServiceImpl(priceDataStorageMock, batchConsumerMock);
        try {

            doNothing().when(batchConsumerMock).startBatchProcessing();
            doNothing().when(batchConsumerMock).uploadBatch(priceDataList);
            doNothing().when(batchConsumerMock).completeBatchProcessing();
            when(batchConsumerMock.getBatchPriceDataStorage()).thenReturn(batchDataStorageMock);
            doNothing().when(priceDataStorageMock).mergeStorages(batchDataStorageMock);


            priceService.startBatch();
            priceService.uploadBatch(priceDataList);

            verify(batchConsumerMock).startBatchProcessing();
            verify(batchConsumerMock).uploadBatch(priceDataList);


        } catch (NotValidStateException e) {

        }
    }

   @Test
    public void priceServiceMainFlowTest() {
        BatchConsumer batchConsumer = new BatchConsumerImpl(new InMemoryPriceDataStorage(), 4);
        PriceService priceService = new PriceServiceImpl(new InMemoryPriceDataStorage(), batchConsumer);
        try {

            batchFullLoad(priceService);

            for (int i = 0; i < 5; i ++){
                assertEquals(i, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
            }


        } catch (NotValidStateException e) {

        }
    }

    private void batchFullLoad(PriceService priceService) throws NotValidStateException {
        priceService.startBatch();
        priceService.uploadBatch(priceDataList);
        priceService.completeBatch();
    }

    @Test
    public void priceServicemainFlowWhenBatchDidntCompleteTest() {
        BatchConsumer batchConsumer = new BatchConsumerImpl(new InMemoryPriceDataStorage(), 4);
        PriceService priceService = new PriceServiceImpl(new InMemoryPriceDataStorage(), batchConsumer);
        try {

            batchFullLoad(priceService);

            List<PriceData> priceDataNewList = makeTestPriceDataWithNewMonthList(0, 7, 8);


            priceService.startBatch();
            priceService.uploadBatch(priceDataNewList);

            for (int i = 0; i < 5; i ++){
                assertEquals(i, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
            }

            priceService.completeBatch();


        } catch (NotValidStateException e) {

        }
    }

    @Test
    public void priceServicemainFlowWhen2BatchesCompleteTest() {
        BatchConsumer batchConsumer = new BatchConsumerImpl(new InMemoryPriceDataStorage(), 4);
        PriceService priceService = new PriceServiceImpl(new InMemoryPriceDataStorage(), batchConsumer);
        try {
            batchFullLoad(priceService);

            List<PriceData> priceDataNewList = makeTestPriceDataWithNewMonthList(0, 7, 8);

            priceService.startBatch();
            priceService.uploadBatch(priceDataNewList);
            priceService.completeBatch();

            for (int i = 0; i < 7; i ++){
                assertEquals(i * 2, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
            }

        } catch (NotValidStateException e) {

        }
    }

    @Test
    public void priceServicemainFlowWhen2BatchCancelsTest() {
        BatchConsumer batchConsumer = new BatchConsumerImpl(new InMemoryPriceDataStorage(), 4);
        PriceService priceService = new PriceServiceImpl(new InMemoryPriceDataStorage(), batchConsumer);
        try {
            batchFullLoad(priceService);
            List<PriceData> priceDataNewList = makeTestPriceDataWithNewMonthList(0, 6, 8);

            priceService.startBatch();
            priceService.uploadBatch(priceDataNewList);
            priceService.cancelBatch();

            for (int i = 0; i < 5; i ++){
                assertEquals(i, priceService.getLatestPriceDataById(String.valueOf(i)).getPayload());
            }

        } catch (NotValidStateException e) {

        }
    }
    @Test
    public void priceServicemainFlowWhen2BatchesInCompleteStateTest() throws Exception {
        BatchConsumer batchConsumer = new BatchConsumerImpl(new InMemoryPriceDataStorage(), 4);
        PriceService priceService = new PriceServiceImpl(new InMemoryPriceDataStorage(), batchConsumer);

        exception.expect(NotValidStateException.class);
        exception.expectMessage("Not a Valid State for the Upload operation");

        batchFullLoad(priceService);

        List<PriceData> priceDataNewList = makeTestPriceDataWithNewMonthList(0, 7, 8);

        priceService.uploadBatch(priceDataNewList);


    }
}