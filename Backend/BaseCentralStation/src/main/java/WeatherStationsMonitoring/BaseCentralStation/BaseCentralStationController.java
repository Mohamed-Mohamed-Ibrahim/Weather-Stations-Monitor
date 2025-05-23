package WeatherStationsMonitoring.BaseCentralStation;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;
import java.util.List;

@RestController
@CrossOrigin
@RequestMapping("weatherMonitoring/BaseCentralStation")

public class BaseCentralStationController {

    @PostMapping
    public String addingRecord(@RequestBody WeatherStatusDto stationStatus) throws IOException {

        System.out.println("Received station ID: " + stationStatus.getStation_id());
        byte[] record = RecordPreparation.buildingRecord(stationStatus) ;
        DatabaseWriter.appendRecord(record, stationStatus.getStation_id(), stationStatus.getStatus_timestamp());
        return "Ok";

    }

    @GetMapping("/view-key")
    public ResponseEntity<KeyValueResponse> getKeyValue(@RequestParam long key) {
        try {
            return ResponseEntity.ok(DatabaseReader.viewKey(key));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }


    @GetMapping("/view-all")
    public ResponseEntity<List<KeyValueResponse>> getAllKeys() {
        try {
            return ResponseEntity.ok(DatabaseReader.viewAll());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
