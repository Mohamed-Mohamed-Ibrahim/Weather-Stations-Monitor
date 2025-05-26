package com.example.DDIAProject;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
@RestController
@CrossOrigin
@RequestMapping("/weatherMonitoring/BaseCentralStation")


public class BaseCentralStationController {

    // In-memory list to store received data (thread-safe alternative is recommended for production)
    private final List<String> stationDataList = new ArrayList<>();

    @PostMapping
    public ResponseEntity<String> receiveStationData(@RequestBody String stationJson) {
        System.out.println("Received station data:\n" + stationJson);

        // Store received JSON string
        stationDataList.add(stationJson);

        return ResponseEntity.ok("Received");
    }

    @GetMapping("/all")
    public ResponseEntity<List<String>> getAllStationData() {
        
        return ResponseEntity.ok(stationDataList);
    }
}