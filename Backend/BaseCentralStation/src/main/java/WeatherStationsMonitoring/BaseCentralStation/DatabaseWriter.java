package WeatherStationsMonitoring.BaseCentralStation;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DatabaseWriter {

    public static class RecordIdentifier {
        private int offset ;
        private int file_id ;
        private long timestamp ;

        private int valueSize ;

        public RecordIdentifier(int offset, int file_id, long timestamp, int valueSize) {
            this.offset = offset;
            this.file_id = file_id;
            this.timestamp = timestamp;
            this.valueSize = valueSize ;
        }

        public int getOffset() {
            return this.offset;
        }

        public int getFile_id() {
            return this.file_id;
        }

        public long getTimestamp() {
            return this.timestamp;
        }

        public int getValueSize() {
            return this.valueSize;
        }

        public void setOffset(int offset) {
            this.offset = offset;
        }

        public void setFile_id(int file_id) {
            this.file_id = file_id;
        }
    }


    private static final String DATABASE_DIRECTORY = "data/BitCask Riak Database/";
    // concurrent hashmap for concurrent access to the same key
    private static final ConcurrentHashMap<Long, RecordIdentifier> keyDirectory = new ConcurrentHashMap<>() ;
    private static final int BATCH_SIZE_FOR_PARQUET = 1 ;

    // multiple from parquet batch to avoid dividing batch on two files ~ 100 MB with average 55 byte / record
    private static final int RECORDS_PER_SEGMENT = 2 * BATCH_SIZE_FOR_PARQUET ;
    // Count of files compacted each time
    private static final int COMPACTION_PERIOD = 3 ;
    private static int activeFileRecords = 0 ;
    private static int activeFileOrder = 1 ;
    private static int activeFileOffset = 0 ;
    private static RandomAccessFile activeFile ;

    public static void setActiveFileOrder(int activeFileOrder) {
        DatabaseWriter.activeFileOrder = activeFileOrder;
    }

    public static void setActiveFileOffset(int activeFileOffset) {
        DatabaseWriter.activeFileOffset = activeFileOffset;
    }

    public static void setActiveFileRecords(int activeFileRecords) {
        DatabaseWriter.activeFileRecords = activeFileRecords;
    }

    @Autowired
    private DatabaseCompactor databaseCompactor ;

//    @PostConstruct
//    public void init() {
//        try {
//            Files.createDirectories(Path.of(DATABASE_DIRECTORY));
//            activeFile = new RandomAccessFile(DATABASE_DIRECTORY+ "Segment_"+ activeFileOrder +".data", "rw");
//            activeFile.seek(activeFileOffset);
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to initialize BitCask file", e);
//        }
//    }

    @PreDestroy
    public void cleanup() {
        try {
            if (activeFile != null)
                activeFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getDatabaseDirectory(){
        return DATABASE_DIRECTORY  ;
    }

    public static ConcurrentHashMap<Long, RecordIdentifier> getKeyDirectory() {
        return keyDirectory;
    }

    public synchronized void appendRecord(byte[] data, long station_id, long timestamp) throws IOException {

        if(activeFile==null){
            Files.createDirectories(Path.of(DATABASE_DIRECTORY));
            activeFile = new RandomAccessFile(DATABASE_DIRECTORY+ "Segment_"+ activeFileOrder +".data", "rw");
            activeFile.seek(activeFileOffset);
        }

        // Append to current file
        activeFile.write(data);

        // merge is atomic operation to handle concurrency
        keyDirectory.merge(
                station_id,
                new RecordIdentifier(activeFileOffset, activeFileOrder, timestamp, data.length-22),
                (oldVal, newVal) -> oldVal.timestamp < newVal.timestamp ? newVal : oldVal
        );

        activeFileOffset += data.length;
        activeFileRecords++ ;

        if(activeFileRecords >= RECORDS_PER_SEGMENT){
            activeFile.close();
            if((activeFileOrder+1) % (COMPACTION_PERIOD+1) == 0){
                databaseCompactor.compactFiles(activeFileOrder-COMPACTION_PERIOD,activeFileOrder);
                activeFileOrder ++ ;
            }
            activeFileOrder++;
            activeFileOffset = 0;
            activeFileRecords = 0 ;
            activeFile = new RandomAccessFile(DATABASE_DIRECTORY+ "Segment_"+ activeFileOrder +".data", "rw");
        }

    }

}
