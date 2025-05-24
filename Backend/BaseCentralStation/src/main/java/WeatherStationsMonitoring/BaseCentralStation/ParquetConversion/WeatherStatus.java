package WeatherStationsMonitoring.BaseCentralStation.ParquetConversion;

import java.util.Random;

public class WeatherStatus {
    long station_id;
    long s_no;
    String battery_status;
    long status_timestamp;
    WeatherStatus.Weather weather;

    static class Weather {
        int humidity;
        int temperature;
        int wind_speed;

        Weather(Random rand) {
            this.humidity = rand.nextInt(101);         // 0–100%
            this.temperature = rand.nextInt(71) + 30;  // 30–100 °F
            this.wind_speed = rand.nextInt(51);        // 0–50 km/h
        }

        public String toJson() {
            return String.format("{\"humidity\":%d,\"temperature\":%d,\"wind_speed\":%d}",
                    humidity, temperature, wind_speed);
        }
    }

    WeatherStatus(long station_id, long s_no, String battery_status, Random rand) {
        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = System.currentTimeMillis() / 1000L;
        this.weather = new WeatherStatus.Weather(rand);
    }

    public String toJson() {
        return String.format(
                "{\"station_id\":%d,\"s_no\":%d,\"battery_status\":\"%s\",\"status_timestamp\":%d,\"weather\":%s}",
                station_id, s_no, battery_status, status_timestamp, weather.toJson());
    }
}