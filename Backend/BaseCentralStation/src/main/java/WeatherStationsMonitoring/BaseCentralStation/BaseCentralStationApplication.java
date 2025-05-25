package WeatherStationsMonitoring.BaseCentralStation;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class BaseCentralStationApplication {

	public static void main(String[] args) {
		SpringApplication.run(BaseCentralStationApplication.class, args);
	}
}
