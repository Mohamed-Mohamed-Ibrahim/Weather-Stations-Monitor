package WeatherStationsMonitoring.BaseCentralStation.ParquetConversion;

import WeatherStationsMonitoring.BaseCentralStation.DatabaseWriter;
import WeatherStationsMonitoring.BaseCentralStation.DatabaseWriter.RecordIdentifier;
import WeatherStationsMonitoring.BaseCentralStation.KeyValueResponse;
import WeatherStationsMonitoring.BaseCentralStation.Message.WeatherStatusMessage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;

import java.nio.file.Paths;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class DatabaseReaderParquet {
    private static String readRecord(int file_id, long offset) throws IOException{

        String fileName = DatabaseWriter.getDatabaseDirectory()+ "Segment_"+ file_id+ ".data";
        RandomAccessFile file = new RandomAccessFile(fileName, "r") ;

        // skip 10 bytes (time stamp & key size since we already know these fields from value
        file.seek(offset+10);
        int valueSize = file.readInt();              // Read value size (4 bytes)
        file.readLong();                            // Read key
        byte[] value = new byte[valueSize];        // Read value
        file.readFully(value);
        return buildValue(WeatherStatusMessage.parseFrom(value)) ;

    }
    private static String buildValue(WeatherStatusMessage ws){
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
    public static KeyValueResponse viewKey(long station_id) throws IOException{
        RecordIdentifier recordIdentifier = DatabaseWriter.getKeyDirectory().get(station_id) ;
        if(recordIdentifier == null){
            throw new IllegalArgumentException("Key not found.");
        }
        String value = readRecord(recordIdentifier.getFile_id(), recordIdentifier.getOffset()) ;
        return new KeyValueResponse(station_id, value) ;
    }


    public static List<KeyValueResponse> viewAll(){
        List<KeyValueResponse> allData = new ArrayList<>() ;
        HashMap<Long, RecordIdentifier> snapshot = new HashMap<>(DatabaseWriter.getKeyDirectory()) ;

        snapshot.forEach((key, recordIdentifier) -> {
            try {
                String value = readRecord(recordIdentifier.getFile_id(), recordIdentifier.getOffset());
                allData.add(new KeyValueResponse(key, value)) ;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return allData ;
    }

    private static final String OUTPUT_DIR = "Parque";

    public static void dumpAllToParquet() throws IOException {
        File dir = new File(DatabaseWriter.getDatabaseDirectory());
        File[] segmentFiles = dir.listFiles((d, name) -> name.startsWith("Segment_") && name.endsWith(".data"));
        MessageType schema = getSchema();
        if (segmentFiles == null) return;

        Map<Long, List<WeatherStatusInfo>> partitionedByStation = new HashMap<>();

        for (File file : segmentFiles) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                while (raf.getFilePointer() < raf.length()) {
                    long timestamp = raf.readLong();             // 8 bytes
                    int keySize = raf.readUnsignedShort();       // 2 bytes
                    int valueSize = raf.readInt();               // 4 bytes

                    if (keySize != 8) {
                        raf.skipBytes(keySize + valueSize);
                        continue;
                    }

                    long key = raf.readLong();                   // 8-byte station_id
                    byte[] value = new byte[valueSize];
                    raf.readFully(value);

                    WeatherStatusMessage ws = WeatherStatusMessage.parseFrom(value);

                    WeatherStatusInfo info = new WeatherStatusInfo(
                            ws.getStationId(),
                            ws.getSNo(),
                            ws.getBatteryStatus().toString().toLowerCase(),
                            ws.getStatusTimestamp()
                    );

                    partitionedByStation
                            .computeIfAbsent(ws.getStationId(), k -> new ArrayList<>())
                            .add(info);
                }
            }
        }

        // Create output directory if it doesn't exist
        File outputDir = new File(OUTPUT_DIR);
        if (!outputDir.exists()) {
            outputDir.mkdir();
        }

        // Write each station's data to a separate Parquet file
        for (Map.Entry<Long, List<WeatherStatusInfo>> entry : partitionedByStation.entrySet()) {
            long stationId = entry.getKey();
            List<WeatherStatusInfo> records = entry.getValue();

            Path path = new Path(Paths.get(OUTPUT_DIR, "station_" + stationId + ".parquet").toString());

            try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
                    .withType(schema)
                    .withConf(new Configuration())
                    .build()) {
                SimpleGroupFactory groupFactory = new SimpleGroupFactory(getSchema());

                for (WeatherStatusInfo info : records) {
                    Group group = groupFactory.newGroup()
                            .append("station_id", info.getStation_id())
                            .append("s_no", info.getS_no())
                            .append("battery_status", info.getBattery_status())
                            .append("status_timestamp", info.getStatus_timestamp());
                    writer.write(group);
                }
            }
        }
    }
    private static MessageType getSchema() {
        return MessageTypeParser.parseMessageType(
                "message WeatherStatusInfo {\n" +
                        " required int64 station_id;\n" +
                        " required int64 s_no;\n" +
                        " required binary battery_status (UTF8);\n" +
                        " required int64 status_timestamp;\n" +
                        "}"
                );
    }


    }
