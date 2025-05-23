package WeatherStationsMonitoring.BaseCentralStation;

public class WeatherStatusDto {

    public enum StatusDto {
        low,
        medium,
        high
    }

    public static class WeatherDto {
        private int humidity ;
        private int temperature ;
        private int wind_speed ;

        public WeatherDto(int humidity, int temperature, int wind_speed) {
            this.humidity = humidity;
            this.temperature = temperature;
            this.wind_speed = wind_speed;
        }

        public int getHumidity() {
            return this.humidity;
        }

        public void setHumidity(int humidity) {
            this.humidity = humidity;
        }

        public int getTemperature() {
            return this.temperature;
        }

        public void setTemperature(int temperature) {
            this.temperature = temperature;
        }

        public int getWind_speed() {
            return this.wind_speed;
        }

        public void setWind_speed(int wind_speed) {
            this.wind_speed = wind_speed;
        }
    }

    private long station_id;
    private long s_no ;
    private StatusDto battery_status ;
    private long status_timestamp ;
    private WeatherDto weather ;

    public WeatherStatusDto(long station_id, long s_no, StatusDto battery_status, long status_timestamp, WeatherDto weather) {
        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = status_timestamp;
        this.weather = weather;
    }

    public long getStation_id() {
        return this.station_id;
    }

    public long getS_no() {
        return this.s_no;
    }

    public StatusDto getBattery_status() {
        return this.battery_status;
    }

    public long getStatus_timestamp() {
        return this.status_timestamp;
    }

    public WeatherDto getWeather() {
        return this.weather;
    }

    public void setStation_id(long station_id) {
        this.station_id = station_id;
    }

    public void setS_no(long s_no) {
        this.s_no = s_no;
    }

    public void setBattery_status(StatusDto battery_status) {
        this.battery_status = battery_status;
    }

    public void setStatus_timestamp(long status_timestamp) {
        this.status_timestamp = status_timestamp;
    }

    public void setWeather(WeatherDto weather) {
        this.weather = weather;
    }
}