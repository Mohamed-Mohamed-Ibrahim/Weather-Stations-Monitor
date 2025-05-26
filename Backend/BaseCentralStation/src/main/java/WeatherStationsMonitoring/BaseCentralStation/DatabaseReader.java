package WeatherStationsMonitoring.BaseCentralStation;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import WeatherStationsMonitoring.BaseCentralStation.DatabaseWriter.RecordIdentifier ;
import WeatherStationsMonitoring.BaseCentralStation.Message.WeatherStatusMessage ;
import org.springframework.stereotype.Service;

@Service
public class DatabaseReader {
    private String readRecord(int file_id, int offset, int valueSize) throws IOException{

        String fileName = DatabaseWriter.getDatabaseDirectory()+ "Segment_"+ file_id+ ".data";
        RandomAccessFile file = new RandomAccessFile(fileName, "r") ;

        // skip 10 bytes (time stamp & key size since we already know these fields from value
        file.seek(offset+22);                   // skip metadata of record
        byte[] value = new byte[valueSize];        // Read value
        file.readFully(value);
        file.close();
        return buildValue(WeatherStatusMessage.parseFrom(value)) ;
    }

    private String buildValue(WeatherStatusMessage ws){
        return "{\n" +
                "   \"station_id\": " + ws.getStationId() + ",\n" +
                "   \"s_no\": " + ws.getSNo() + ",\n" +
                "   \"battery_status\": " + "\"" + ws.getBatteryStatus().toString().toLowerCase() + "\",\n" +
                "   \"status_timestamp\": " + ws.getStatusTimestamp() + ",\n" +
                "   \"weather\":  {\n" +
                "       \"humidity\": " + ws.getWeather().getHumidity() + ",\n" +
                "       \"temperature\": " + ws.getWeather().getTemperature() + ",\n" +
                "       \"wind_speed\": " + ws.getWeather().getWindSpeed() + "\n" +
                "   }\n" +
                "}" ;
    }
    public KeyValueResponse viewKey(long station_id) throws IOException{
        RecordIdentifier recordIdentifier = DatabaseWriter.getKeyDirectory().get(station_id) ;
        if(recordIdentifier == null){
            throw new IllegalArgumentException("Key not found.");
        }
        String value = readRecord(recordIdentifier.getFile_id(), recordIdentifier.getOffset(), recordIdentifier.getValueSize()) ;
        return new KeyValueResponse(station_id, value) ;
    }


    public List<KeyValueResponse> viewAll(){
        List<KeyValueResponse> allData = new ArrayList<>() ;
        HashMap<Long, RecordIdentifier> snapshot = new HashMap<>(DatabaseWriter.getKeyDirectory()) ;

        snapshot.forEach((key, recordIdentifier) -> {
            try {
                String value = readRecord(recordIdentifier.getFile_id(), recordIdentifier.getOffset(), recordIdentifier.getValueSize());
                allData.add(new KeyValueResponse(key, value)) ;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return allData ;
    }

}
