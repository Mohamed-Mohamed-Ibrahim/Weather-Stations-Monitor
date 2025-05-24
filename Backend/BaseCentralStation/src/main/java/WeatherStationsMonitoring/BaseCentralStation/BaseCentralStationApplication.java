package WeatherStationsMonitoring.BaseCentralStation;
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
	CommandLineRunner commandLineRunner () {
		return args -> {
			System.out.println(1231231);
//			System.out.println();
//			DatabaseReader.viewAllFromDisk().forEach(x -> {
//						System.out.println(x.getValue());
//			}

//			);
		};
	}
}
