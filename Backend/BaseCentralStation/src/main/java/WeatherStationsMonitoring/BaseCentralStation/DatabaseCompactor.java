package WeatherStationsMonitoring.BaseCentralStation;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import WeatherStationsMonitoring.BaseCentralStation.DatabaseWriter.RecordIdentifier ;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class DatabaseCompactor {
    private static final int BUFFER_SIZE = 1024*1024;   // 1MB -> for chunk reading, share between any compaction process
    private static final HashMap<Long, RecordIdentifier> compactionHashmap = new HashMap<>() ; // for each compaction process
    /* to be shared between all chunks of each file , we reset them before each file */
    private static int countOfSkippedBytes = 0 ;
    private static long currentRecordOffset = 0 ;

    // Async for background process
    @Async
    public void compactFiles(int start, int end) throws IOException {

        for(int i=start ; i<= end ; i++){
            if(i==0)
                continue;
            // prepare for new file
            countOfSkippedBytes = 0 ;
            currentRecordOffset = 0 ;
            processFile(i) ;
        }

        writeCompactedFile(end+1);  // compacted file is the next file to last file in compaction

        // merge compactionHashmap with hashmap for whole system
        // atomic modification
        for (Long key : compactionHashmap.keySet()) {
            RecordIdentifier recordIdentifier = compactionHashmap.get(key);
            DatabaseWriter.getKeyDirectory().merge(
                    key,
                    recordIdentifier,
                    (oldVal, newVal) -> oldVal.getTimestamp() <= newVal.getTimestamp() ? newVal : oldVal
            );
        }

        // remove original files
        for(int i=start ; i<= end ; i++){
            Path path = Path.of(DatabaseWriter.getDatabaseDirectory()+ "Segment_"+ i+ ".data");
            if(Files.exists(path))
                Files.delete(path) ;
        }

        compactionHashmap.clear();      // clear hashmap for next compaction operation

        for (Long key : DatabaseWriter.getKeyDirectory().keySet()) {
            System.out.print(key+"        ");
            RecordIdentifier recordIdentifier = DatabaseWriter.getKeyDirectory().get(key);
            System.out.print(recordIdentifier.getFile_id()+"       ");
            System.out.println(recordIdentifier.getOffset());
        }
    }

    /*
        we force the chunk to start from beginning of record :
        if the last part of the previous chunk don't match the full record we have two possibilities :
            1- the first 22 bytes that identify record are present in the previous chunk, so we add it
               to the hashmap and skip remaining value bytes from new chunk and start the new chunk from the next record.

            2- the whole 22 bytes aren't present, se we append this incomplete record to the beginning of the new chunk
               and start new chunk from this record.
    */

    private void processFile(int fileID) throws IOException {
        // open file
        String fileName = DatabaseWriter.getDatabaseDirectory()+ "Segment_"+ fileID+ ".data";
        RandomAccessFile file = new RandomAccessFile(fileName, "r");
        FileChannel channel = file.getChannel();
        ByteBuffer chunkBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        byte[] leftover = new byte[0];  // Holds leftover bytes (bytes left from previous chunk)

        // split file into chunks
        while (channel.read(chunkBuffer) > 0) {
            chunkBuffer.flip();  // Prepare buffer for reading

            // Combine leftover + new chunk
            ByteBuffer combinedBuffer = ByteBuffer.allocate(leftover.length + chunkBuffer.remaining());
            combinedBuffer.put(leftover);  // Append leftover bytes first
            combinedBuffer.put(chunkBuffer);  // Append new chunk bytes
            combinedBuffer.flip();

            combinedBuffer.position(countOfSkippedBytes) ;      // skipping non-needed bytes
            countOfSkippedBytes = 0 ;                           // reset countOfSkippedBytes to 0 for next chunk
            leftover = processChunk(combinedBuffer, fileID);
            chunkBuffer.clear() ;   // Prepare buffer for next read
        }
        // close file
        channel.close();
        file.close();
    }

    private byte[] processChunk(ByteBuffer combinedBuffer, int fileID){

        while(combinedBuffer.hasRemaining()){
            // record isn't full , combine it to next chunk, force new chunk to start from record
            if(combinedBuffer.remaining() < 22) {   // less than time stamp + key & value size + key
                byte[] leftover = new byte[combinedBuffer.remaining()] ;
                combinedBuffer.get(leftover) ;
                return leftover;
            }

            long recordTimestamp = combinedBuffer.getLong() ;   // get time stamp
            combinedBuffer.get() ; combinedBuffer.get() ;       // skip key size
            int recordValueSize = combinedBuffer.getInt() ;     // get value size
            long recordKey = combinedBuffer.getLong() ;         // get key

            /* add to map*/
            compactionHashmap.merge(
                    recordKey,
                    new RecordIdentifier(currentRecordOffset, fileID, recordTimestamp, recordValueSize),
                    (oldVal, newVal) -> oldVal.getTimestamp() < newVal.getTimestamp() ? newVal : oldVal
            );

            currentRecordOffset += 22 + recordValueSize ;   // move offset after current record

            // skip value because we don't need it, we only get offset
            if(combinedBuffer.remaining() < recordValueSize) {
                countOfSkippedBytes = recordValueSize-combinedBuffer.remaining() ;
                break;
            }
            // move position after key and enter new iteration
            combinedBuffer.position(combinedBuffer.position() + recordValueSize) ;

        }
        return new byte[0] ;    // since we reached here, there is no leftover bytes so return empty array
    }


    private void writeCompactedFile(int compactedFileID) throws IOException {

        RandomAccessFile compactedFile = new RandomAccessFile(DatabaseWriter.getDatabaseDirectory() + "Segment_" + compactedFileID + ".data", "rw");
        long offsetInCompactedFile = 0 ;    // offset for records from compacted file


        for (Long key : compactionHashmap.keySet()) {
            RecordIdentifier recordIdentifier = compactionHashmap.get(key) ;
            // read the record from old files (before compaction)
            String fileName = DatabaseWriter.getDatabaseDirectory()+ "Segment_"+ recordIdentifier.getFile_id()+ ".data";
            RandomAccessFile file = new RandomAccessFile(fileName, "r") ;
            file.seek(recordIdentifier.getOffset());
            byte[] data = new byte[22+recordIdentifier.getValueSize()] ;
            file.readFully(data);

            // read in compacted file
            compactedFile.write(data);
            // edit record identifier by new information for compacted file
            recordIdentifier.setOffset(offsetInCompactedFile);
            recordIdentifier.setFile_id(compactedFileID);
            offsetInCompactedFile += data.length ;      // move offset
            file.close();
        }
        compactedFile.close();

    }
}
