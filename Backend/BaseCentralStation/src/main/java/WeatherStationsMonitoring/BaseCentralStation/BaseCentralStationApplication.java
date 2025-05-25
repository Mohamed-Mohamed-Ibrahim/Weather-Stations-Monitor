package WeatherStationsMonitoring.BaseCentralStation;

import WeatherStationsMonitoring.BaseCentralStation.Consumer.PollingEndpoint;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class BaseCentralStationApplication {

	public static void main(String[] args) {
		SpringApplication.run(BaseCentralStationApplication.class, args);
	}

	@Bean
	CommandLineRunner commandLineRunner() {
		return args -> {
			System.out.println("Program Started");

			PollingEndpoint pollingEndpoint = new PollingEndpoint();
			Thread thread = new Thread(pollingEndpoint);
			thread.start();

			System.out.println("PollingEndpoint thread started.");
		};
	}
}
