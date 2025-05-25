package com.example.DDIAProject;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling
public class DdiaProjectApplication {

	public static void main(String[] args) {

		SpringApplication.run(DdiaProjectApplication.class, args);


	}
}