package temp;

import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyConverter {

  private static final int STATION_ID_LENGTH = 12;

  /**
   * @return A row key whose format is: <station_id> <reverse_order_epoch>
   */
  public static byte[] makeObservationRowKey(String stationId,
      long observationTime) {
    byte[] row = new byte[STATION_ID_LENGTH + Bytes.SIZEOF_LONG];
    Bytes.putBytes(row, 0, Bytes.toBytes(stationId), 0, STATION_ID_LENGTH);
    long reverseOrderEpoch = Long.MAX_VALUE - observationTime;
    Bytes.putLong(row, STATION_ID_LENGTH, reverseOrderEpoch);
    return row;
  }
  
  public static byte[] makeFlightRowKey(int flightId, int year, int month, int date, long depTime) {
	    byte[] row = new byte[Bytes.SIZEOF_INT * 4 + Bytes.SIZEOF_LONG];
	    Bytes.putInt(row, 0, flightId);
	    Bytes.putInt(row, Bytes.SIZEOF_INT, year);
	    Bytes.putInt(row, Bytes.SIZEOF_INT*2, month);
	    Bytes.putInt(row, Bytes.SIZEOF_INT*3, date);
	    Bytes.putLong(row, Bytes.SIZEOF_INT*4, depTime);
	    return row;
	  }
}
