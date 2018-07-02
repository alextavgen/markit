package markit.models;

import lombok.NonNull;

import java.time.LocalDateTime;


public class PriceData {

    private final @NonNull String id;

    private final @NonNull LocalDateTime asOf;

    private final @NonNull Object payload;

    public PriceData(String id, LocalDateTime asOf, Object payload) {
        this.id = id;
        this.asOf = asOf;
        this.payload = payload;
    }

    @NonNull
    public String getId() {
        return this.id;
    }

    @NonNull
    public LocalDateTime getAsOf() {
        return this.asOf;
    }

    @NonNull
    public Object getPayload() {
        return this.payload;
    }
}


