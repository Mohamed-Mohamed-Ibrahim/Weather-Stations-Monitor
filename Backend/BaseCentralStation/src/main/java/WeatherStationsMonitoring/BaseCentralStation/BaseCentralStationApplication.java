package WeatherStationsMonitoring.BaseCentralStation;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableAsync
public class BaseCentralStationApplication {

	public static void main(String[] args) {
		SpringApplication.run(BaseCentralStationApplication.class, args);
	}

	@Bean
	CommandLineRunner commandLineRunner () {
		return args -> {
			System.out.println("Program Started");



		};
	}
}
