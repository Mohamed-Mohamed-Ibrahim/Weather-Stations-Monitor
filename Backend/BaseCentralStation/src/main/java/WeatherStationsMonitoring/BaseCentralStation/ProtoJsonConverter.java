package WeatherStationsMonitoring.BaseCentralStation;
import WeatherStationsMonitoring.BaseCentralStation.Message.Weather;
import WeatherStationsMonitoring.BaseCentralStation.Message.Status;
import WeatherStationsMonitoring.BaseCentralStation.Message.WeatherStatusMessage;

public class ProtoJsonConverter {
    public static WeatherStatusMessage jsonToProtoConverter(WeatherStatusDto ws) {

        Weather.Builder weatherBuilder = Weather.newBuilder()
                .setHumidity(ws.getWeather().getHumidity())
                .setTemperature(ws.getWeather().getTemperature())
                .setWindSpeed(ws.getWeather().getWind_speed());

        Status status;
        switch(ws.getBattery_status().toString().toUpperCase()) {
            case "MEDIUM": status = Status.MEDIUM; break;
            case "HIGH": status = Status.HIGH; break;
            default: status = Status.LOW; // default/fallback
        }

        return WeatherStatusMessage.newBuilder()
                .setStationId(ws.getStation_id())
                .setSNo(ws.getS_no())
                .setBatteryStatus(status)
                .setStatusTimestamp(ws.getStatus_timestamp())
                .setWeather(weatherBuilder)
                .build();
    }
}
