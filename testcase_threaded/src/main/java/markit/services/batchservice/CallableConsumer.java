package markit.services.batchservice;

import markit.models.PriceData;
import markit.storage.InMemoryPriceDataStorage;
import markit.storage.PriceDataStorage;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class CallableConsumer implements Callable<PriceDataStorage> {

    /**
     *  Lambda which compares two PriceData and returns with a latest time
     */
    private final BiFunction<PriceData, PriceData, PriceData> comparingPriceDataBiFunction =
            (oldValue, newValue) -> newValue.getAsOf().compareTo(oldValue.getAsOf()) >= 0 ? newValue: oldValue;

    private final ConcurrentMap<String, PriceData> priceMap;

    private final List<PriceData> priceDataList;

    final static Logger logger = Logger.getLogger(CallableConsumer.class);

    public CallableConsumer(List<PriceData> priceDataList){
        this.priceMap = new ConcurrentHashMap<>();
        this.priceDataList = priceDataList;

    }
    /**
     *  MEthod for run in thread which transforms list of PriceData into ConcurrentMap
     */
    public PriceDataStorage call() {
        logger.debug("Consumer " + Thread.currentThread().getId()
                + " started job");
        // Imperative version
        /* Map<String, PriceData> batchMap = new ConcurrentHashMap<>();


        PriceData priceDataElement;
        for(PriceData priceData: priceDataList){
            priceDataElement = batchMap.get(priceData.getId());
            if (priceDataElement == null){
                batchMap.put(priceData.getId(), priceData);
            }
            else {
                if (priceData.getAsOf().compareTo((priceDataElement.getAsOf())) >= 0){
                    batchMap.put(priceData.getId(), priceData);
                }
             }
        }
         */

        logger.debug("Start processing price data list with size:  " + priceDataList.size());

        ConcurrentMap<String, PriceData> batchMap =  priceDataList.stream().collect(Collectors.toConcurrentMap(PriceData::getId,
                p -> p,
                (oldValue, newValue) -> newValue.getAsOf().compareTo(oldValue.getAsOf()) >= 0 ? newValue: oldValue));


        logger.debug("Consumer " + Thread.currentThread().getId()
                + ": task is done. Map size is " + priceMap.size());


        return new InMemoryPriceDataStorage(batchMap);
    }
}