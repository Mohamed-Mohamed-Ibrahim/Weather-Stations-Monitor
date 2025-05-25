package WeatherStationsMonitoring.BaseCentralStation;
import java.nio.ByteBuffer;
import WeatherStationsMonitoring.BaseCentralStation.Message.*  ;
import org.springframework.stereotype.Component;

@Component
public class RecordPreparation {
    private static byte[] longToBytes(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES); // 8 bytes
        buffer.putLong(value);
        return buffer.array();
    }
    private static byte[] intToBytes(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES); // 4 bytes
        buffer.putInt(value);
        return buffer.array();
    }

     public static byte[] buildingRecord (WeatherStatusDto ws) {

         // Serializing object to represent it as value
         WeatherStatusMessage message = ProtoJsonConverter.jsonToProtoConverter(ws) ;
         byte[] value = message.toByteArray() ;

         /* record size = 8 bytes for timestamp + 2 bytes for key size + 4 bytes for value size +
                          8 bytes for key (long) + size of serialized data */
         int record_size = 8 + 2 + 4 + 8 + value.length ;
         byte[] record = new byte[record_size] ;

         System.arraycopy(longToBytes(ws.getStatus_timestamp()), 0, record, 0, 8);       // writing timestamp
         record [8] = 0 ; record [9] = 8 ;                                                                  // writing key size
         System.arraycopy(intToBytes(value.length), 0, record, 10, 4);                  // writing value size
         System.arraycopy(longToBytes(ws.getStation_id()), 0, record, 14, 8);          // writing key
         System.arraycopy(value, 0, record, 22, value.length);                              // writing value

         return record ;
     }
}
