package markit.services.batchservice;

import markit.exceptions.NotValidStateException;
import markit.models.PriceData;
import markit.storage.PriceDataStorage;
import org.apache.log4j.Logger;

import java.util.List;


public class BatchConsumerImpl implements BatchConsumer {

    final static Logger logger = Logger.getLogger(BatchConsumerImpl.class);

    private BatchRunState state;

    private final PriceDataStorage storage;

    public BatchConsumerImpl(PriceDataStorage storage){
        this.storage = storage;
        this.state = BatchRunState.INITIALISATION;
    }

    @Override
    public void startBatchProcessing() throws NotValidStateException{
        if (this.state == BatchRunState.INITIALISATION || this.state == BatchRunState.CANCELED){
            this.state = BatchRunState.STARTED;
        }
        else {
            logger.error("Not a valid state for batch operation from " + this.state + " to " + BatchRunState.STARTED);
            throw new NotValidStateException( "Not a Valid State for the Start Batch operation");
        }

    }

    @Override
    public void uploadBatch(List<PriceData> priceDataList) throws NotValidStateException {
        if (this.state == BatchRunState.STARTED) {
            this.state = BatchRunState.UPLOAD;
        }

        if (this.state == BatchRunState.UPLOAD) {
            // One method
            // priceDataList.forEach(storage::setPriceData);
            storage.setPriceDataAsList(priceDataList);
        }
        else {
            logger.error("Not a valid state for batch operation from " + this.state + " to " + BatchRunState.UPLOAD);
            throw new NotValidStateException( "Not a Valid State for the Upload operation");
        }

    }

    @Override
    public void completeBatchProcessing() throws NotValidStateException{
        if (this.state ==BatchRunState.UPLOAD){
            this.state = BatchRunState.COMPLETED;
        }
        else {
            logger.error("Not a valid state for batch operation from " + this.state + " to " + BatchRunState.COMPLETED);
            throw new NotValidStateException( "Not a Valid State for the Complete operation");
        }

    }

    @Override
    public void cancelBatchProcessing() throws NotValidStateException{
        if (this.state ==BatchRunState.UPLOAD){
            this.state = BatchRunState.CANCELED;
            this.storage.init();
        }
        else {
            logger.error("Not a valid state for batch operation from " + this.state + " to " + BatchRunState.CANCELED);
            throw new NotValidStateException( "Not a Valid State for the Cancel operation");
        }

    }

    @Override
    public PriceDataStorage getBatchPriceDataStorage() throws NotValidStateException{
        if (this.state == BatchRunState.COMPLETED){
            return this.storage;
        }
        else {
            logger.error("Not a valid state for batch operation from " + this.state);
            throw new NotValidStateException("Not a Valid State for the ReturnBatchStorage operation");
        }
    }
}
