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

    private static final String OUTPUT_DIR = "data/Parquet";

    public static void dumpAllToParquet() throws IOException {
        File dir = new File(DatabaseWriter.getDatabaseDirectory());
        File[] segmentFiles = dir.listFiles((d, name) -> name.startsWith("Segment_") && name.endsWith(".data"));
        MessageType schema = getSchema();
        if (segmentFiles == null) return;

        File outputDir = new File(OUTPUT_DIR);
        if (!outputDir.exists()) {
            outputDir.mkdir();
        }

        int batchSize = 10_000;
        int batchCount = 0;
        int recordCount = 0;

        ParquetWriter<Group> writer = null;
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        try {
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

                        if (writer == null) {
                            Path path = new Path(Paths.get(OUTPUT_DIR, batchCount + ".parquet").toString());
                            writer = ExampleParquetWriter.builder(path)
                                    .withType(schema)
                                    .withConf(new Configuration())
                                    .build();
                        }

                        Group group = groupFactory.newGroup()
                                .append("station_id", ws.getStationId())
                                .append("s_no", ws.getSNo())
                                .append("battery_status", ws.getBatteryStatus().toString().toLowerCase())
                                .append("status_timestamp", ws.getStatusTimestamp());

                        writer.write(group);
                        recordCount++;

                        if (recordCount >= batchSize) {
                            writer.close();
                            writer = null;
                            recordCount = 0;
                            batchCount++;
                        }
                    }
                }
            }
        } finally {
            if (writer != null) {
                writer.close();
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
