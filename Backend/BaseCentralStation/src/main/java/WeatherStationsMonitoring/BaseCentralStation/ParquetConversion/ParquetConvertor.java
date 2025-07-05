package WeatherStationsMonitoring.BaseCentralStation.ParquetConversion;

import WeatherStationsMonitoring.BaseCentralStation.DatabaseReader;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParquetConvertor {

//    public static void main(String[] args) throws IOException {
//        System.out.println();;
//        DatabaseReaderParquet databaseReaderParquet = new DatabaseReaderParquet();
//        databaseReaderParquet.start();
//        try {
//            Thread.sleep(100);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        System.out.println(123123);
//        return;
//    }
public static void main(String[] args) {
    Runnable task = () -> System.out.println("Running on " + Thread.currentThread().getName());

    ExecutorService executor = Executors.newFixedThreadPool(2);

    for (int i = 0; i < 5; i++) {
        executor.submit(task);
    }

    executor.shutdown();
}
}
