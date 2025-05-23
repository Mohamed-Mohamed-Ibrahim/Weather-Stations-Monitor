package WeatherStationsMonitoring.BaseCentralStation;

public class KeyValueResponse {
    private long key;
    private String value;

    public KeyValueResponse(long key, String value) {
        this.key = key;
        this.value = value;
    }

    // Getters
    public long getKey() { return this.key; }
    public String getValue() { return this.value; }
}

