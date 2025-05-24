package WeatherStationsMonitoring.BaseCentralStation.ParquetConversion;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Random;


public class WeatherStatusInfo {
    long station_id;
    long s_no;
    String battery_status;
    long status_timestamp;

    public WeatherStatusInfo(long station_id, long s_no, String battery_status, long status_timestamp) {
        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = status_timestamp;
    }

    public long getStation_id() {
        return station_id;
    }

    public long getS_no() {
        return s_no;
    }

    public String getBattery_status() {
        return battery_status;
    }

    public long getStatus_timestamp() {
        return status_timestamp;
    }
}