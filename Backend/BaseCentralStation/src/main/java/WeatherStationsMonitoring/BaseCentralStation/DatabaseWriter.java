package WeatherStationsMonitoring.BaseCentralStation;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseWriter {

    public static class RecordIdentifier {
        private long offset ;
        private int file_id ;
        private long timestamp ;

        public RecordIdentifier(long offset, int file_id, long timestamp) {
            this.offset = offset;
            this.file_id = file_id;
            this.timestamp = timestamp;
        }

        public long getOffset() {
            return this.offset;
        }

        public int getFile_id() {
            return this.file_id;
        }

        public long getTimestamp() {
            return this.timestamp;
        }
    }


    private static final String DATABASE_DIRECTORY = "data/BitCask Riak Database/";
    // concurrent hashmap for concurrent access to the same key
    private static final ConcurrentHashMap<Long, RecordIdentifier> keyDirectory = new ConcurrentHashMap<>() ;
    private static final long SEGMENT_SIZE = 134217728 ;        // segment size = 128 MB
    private static int activeFileOrder = 1 ;
    private static long activeFileOffset = 0 ;

    public static String getDatabaseDirectory(){
        return DATABASE_DIRECTORY ;
    }

    public static ConcurrentHashMap<Long, RecordIdentifier> getKeyDirectory() {
        return keyDirectory;
    }

    public static synchronized void appendRecord(byte[] data, long station_id, long timestamp) throws IOException {

        if (data.length + activeFileOffset > SEGMENT_SIZE) {
            activeFileOrder++;
            activeFileOffset = 0;
        }

        Files.createDirectories(Path.of(DATABASE_DIRECTORY));
        Path path = Path.of(DATABASE_DIRECTORY , "Segment_" + activeFileOrder + ".data");
        Files.write(path, data, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        // merge is atomic operation to handle concurrency
        keyDirectory.merge(
                station_id,
                new RecordIdentifier(activeFileOffset, activeFileOrder, timestamp),
                (oldVal, newVal) -> oldVal.timestamp < newVal.timestamp ? newVal : oldVal
        );


        activeFileOffset += data.length;
    }

}
