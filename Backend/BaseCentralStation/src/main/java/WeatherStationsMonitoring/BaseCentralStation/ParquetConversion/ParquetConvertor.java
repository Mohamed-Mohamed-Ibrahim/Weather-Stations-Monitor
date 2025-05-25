package WeatherStationsMonitoring.BaseCentralStation.ParquetConversion;

import WeatherStationsMonitoring.BaseCentralStation.DatabaseReader;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class ParquetConvertor {

    public static void main(String[] args) throws IOException {
        System.out.println();;
        DatabaseReaderParquet.dumpAllToParquet();
        return;
    }
}
