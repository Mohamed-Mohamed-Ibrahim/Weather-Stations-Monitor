package WeatherStationsMonitoring.BaseCentralStation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import WeatherStationsMonitoring.BaseCentralStation.DatabaseWriter.RecordIdentifier ;
import WeatherStationsMonitoring.BaseCentralStation.DatabaseCompactor;


@Component
public class DatabaseRecovery implements SmartLifecycle {

    @Autowired
    DatabaseCompactor databaseCompactor ;

    private boolean running = false;

    @Override
    public void start() throws RuntimeException {
        System.out.println("=== Reconstructing keydir before server starts ===");

        File[] files = new File(DatabaseWriter.getDatabaseDirectory()).listFiles();
        if(files.length == 0){
            running = true ;
            return ;
        }

        Path path = Path.of(DatabaseWriter.getDatabaseDirectory()+ "hint_file.data");
        int[] fileNames ;
        if(Files.exists(path)) {
            fileNames = new int[files.length-1] ;
            int i = 0 ;
            for(File file : files) {
                String name = file.getName() ;
                if(!name.equals("hint_file.data")) {
                    name = name.substring(8, name.length()-5) ;
                    fileNames[i] = Integer.parseInt(name);
                    i++;
                }
            }
        }

        else {
            fileNames = new int[files.length] ;
            int i = 0 ;
            for(File file : files) {
                String name = file.getName() ;
                name = name.substring(8, name.length()-5) ;
                fileNames[i] = Integer.parseInt(name);
                i++;
            }
            Arrays.sort(fileNames);
        }



        if(Files.exists(path)) {
            processHint(fileNames[0]);
            for(int i=1 ; i<fileNames.length; i++) {
                try {
                    databaseCompactor.processFile(fileNames[i], DatabaseWriter.getKeyDirectory());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }
        else{
            for(int i=0 ; i<fileNames.length; i++) {
                try {
                    databaseCompactor.processFile(fileNames[i], DatabaseWriter.getKeyDirectory());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        DatabaseWriter.setActiveFileOffset(DatabaseCompactor.getCurrentRecordOffset());
        DatabaseWriter.setActiveFileOrder(fileNames[fileNames.length-1]);
        DatabaseWriter.setActiveFileRecords(DatabaseCompactor.getTotalNumberOfRecords());
        running = true;
    }

    private void processHint(int hintID){
        try{
            RandomAccessFile hint_file = new RandomAccessFile(DatabaseWriter.getDatabaseDirectory() + "hint_file.data", "r");
            FileChannel channel = hint_file.getChannel();
            ByteBuffer chunkBuffer = ByteBuffer.allocate(1024*1024);
            byte[] leftover = new byte[0];  // Holds leftover bytes (bytes left from previous chunk)

            // split file into chunks
            while (channel.read(chunkBuffer) > 0) {

                chunkBuffer.flip();  // Prepare buffer for reading
                // Combine leftover + new chunk
                ByteBuffer combinedBuffer = ByteBuffer.allocate(leftover.length + chunkBuffer.remaining());
                combinedBuffer.put(leftover);  // Append leftover bytes first
                combinedBuffer.put(chunkBuffer);  // Append new chunk bytes
                combinedBuffer.flip();

                leftover = extractHintFile(combinedBuffer, hintID) ;

                chunkBuffer.clear() ;   // Prepare buffer for next read

            }

            channel.close();
            hint_file.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private byte[] extractHintFile(ByteBuffer combinedBuffer, int hintID){

        while(combinedBuffer.hasRemaining()){
            // record isn't full , combine it to next chunk, force new chunk to start from record
            if(combinedBuffer.remaining() < 26) {   // less than record size
                byte[] leftover = new byte[combinedBuffer.remaining()] ;
                combinedBuffer.get(leftover) ;
                return leftover;
            }

            long recordTimestamp = combinedBuffer.getLong() ;   // get time stamp
            combinedBuffer.get() ; combinedBuffer.get() ;       // skip key size
            int recordValueSize = combinedBuffer.getInt() ;     // get value size
            int recordOffset = combinedBuffer.getInt() ;
            long recordKey = combinedBuffer.getLong() ;         // get key

            DatabaseWriter.getKeyDirectory().put(recordKey,
                    new RecordIdentifier(recordOffset, hintID, recordTimestamp, recordValueSize)) ;

        }

        return new byte[0] ;

    }


    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true; // Start automatically
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE; // Run early
    }

}
