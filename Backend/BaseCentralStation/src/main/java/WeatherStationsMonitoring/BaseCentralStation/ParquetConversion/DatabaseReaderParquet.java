package WeatherStationsMonitoring.BaseCentralStation.ParquetConversion;

import WeatherStationsMonitoring.BaseCentralStation.DatabaseWriter;
import WeatherStationsMonitoring.BaseCentralStation.DatabaseWriter.RecordIdentifier;
import WeatherStationsMonitoring.BaseCentralStation.KeyValueResponse;
import WeatherStationsMonitoring.BaseCentralStation.Message;
import WeatherStationsMonitoring.BaseCentralStation.Message.WeatherStatusMessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


import jakarta.annotation.PostConstruct;
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
import org.springframework.stereotype.Service;

//@Service
public class DatabaseReaderParquet implements Runnable {

    private Thread t;

    @PostConstruct
    public void init() {
        outputDir = new File(Parquet_DIRECTORY);
        if (!outputDir.exists()) {
            outputDir.mkdir();
        }

    }

    public DatabaseReaderParquet(String activeFilPath, Integer activeFileOrder) {
        try {
            this.activeFile = new RandomAccessFile(activeFilPath, "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.activeFileOrder = activeFileOrder;
    }

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

    private static final String OUTPUT_DIR = "Parquet";

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

    private RandomAccessFile activeFile;
    private Integer activeFileOrder;
    private static final String Parquet_DIRECTORY = "Parquet/";
    MessageType schema = getSchema();
    File outputDir;

    private void parquetConversion() throws IOException {

        activeFile.seek(0);
        List<WeatherStatusInfo> partitionedByStation = new ArrayList<>();
        while (activeFile.getFilePointer() < activeFile.length()) {
            long ts = activeFile.readLong();             // 8 bytes
            int keySize = activeFile.readUnsignedShort();       // 2 bytes
            int valueSize = activeFile.readInt();               // 4 bytes

            if (keySize != 8) {
                activeFile.skipBytes(keySize + valueSize);
                continue;
            }

            long key = activeFile.readLong();                   // 8-byte station_id
            byte[] value = new byte[valueSize];
            activeFile.readFully(value);

            Message.WeatherStatusMessage ws = Message.WeatherStatusMessage.parseFrom(value);

            WeatherStatusInfo info = new WeatherStatusInfo(
                    ws.getStationId(),
                    ws.getSNo(),
                    ws.getBatteryStatus().toString().toLowerCase(),
                    ws.getStatusTimestamp()
            );

            partitionedByStation.add(info);
        }

        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(Paths.get(Parquet_DIRECTORY, activeFileOrder+".parquet").toString());

        // Write each station's data to a separate Parquet file
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
                .withType(schema)
                .withConf(new Configuration())
                .build()) {
            SimpleGroupFactory groupFactory = new SimpleGroupFactory(getSchema());
            for  (WeatherStatusInfo info : partitionedByStation) {



                Group group = groupFactory.newGroup()
                        .append("station_id", info.getStation_id())
                        .append("s_no", info.getS_no())
                        .append("battery_status", info.getBattery_status())
                        .append("status_timestamp", info.getStatus_timestamp());
                writer.write(group);
            }
        }

        this.activeFile.close();

    }

    @Override
    public void run() {
        System.out.println("Parquet Conversion "+ activeFileOrder +" has begun");
        try {
//            dumpAllToParquet();
            parquetConversion();

        } catch (IOException e) {

            throw new RuntimeException(e);
        }
        System.out.println("Parquet Conversion "+ activeFileOrder +" has ended");
    }

    public void start() throws FileNotFoundException {
        if (t == null) {
            t = new Thread(this);
            t.start();
        }
    }
}
