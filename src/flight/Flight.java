package flight;

import org.apache.hadoop.hbase.util.Bytes;

public class Flight {

	static final byte[] FLIGHT_FAMILY_AIRLINE = Bytes.toBytes("airline");
	static final byte[] FLIGHT_FAMILY_ARRDELAY = Bytes.toBytes("delay");
	static final byte[] FLIGHT_FAMILY_DATE = Bytes.toBytes("date");
	static final byte[] FLIGHT_FAMILY_DEPTIME = Bytes.toBytes("depTime");
	static final byte[] FLIGHT_FAMILY_ISCANCEL = Bytes.toBytes("iscancel");
	static final byte[] FLIGHT_FAMILY_ISDIVERTED = Bytes.toBytes("diverted");
	
	static final byte[] FLIGHT_FAMILY_ARR = Bytes.toBytes("arrive");
	static final byte[] FLIGHT_FAMILY_DEP = Bytes.toBytes("dept");
	static final byte[] FLIGHT_FAMILY_ORIGIN = Bytes.toBytes("origin");
	static final byte[] FLIGHT_FAMILY_DEST = Bytes.toBytes("dest");
	static final byte[] FLIGHT_FAMILY_OTHER = Bytes.toBytes("other");
	static final byte[] FLIGHT_FAMILY_INFO = Bytes.toBytes("information");
	static final byte[] FLIGHT_FAMILY_DELAYS = Bytes.toBytes("delays");
	
	private int airlineID;
	private int year;
	private int month;
	private int date;
	private long depTime;
	private double arrDelayMinutes;
	private boolean isCancel;
	private boolean isDiverted;

	/**
	 * Origin, Dest, FlightDate, ArrTime, DepTime, Cancelled != 1, Diverted != 1, ArrDelayMinutes, FlightNum,
	 * Year, Month
	 */
	public Flight(int airlineID, int year, int month, int date, long depTime, double arrDelayMinutes, boolean isCancel, boolean isDiverted) {
		this.airlineID = airlineID;
		this.year = year;
		this.month = month;
		this.date = date;
		this.depTime = depTime;
		this.arrDelayMinutes = arrDelayMinutes;
		this.isCancel = isCancel;
		this.isDiverted = isDiverted;
	}

	/**
	 * @return the flightNum
	 */
	public int getAirlineID() {
		return airlineID;
	}

	/**
	 * @return the year of flight
	 */
	public int getYear() {
		return year;
	}
	
	/**
	 * @return the month of flight
	 */
	public int getMonth() {
		return month;
	}
	
	/**
	 * @return the date of flight
	 */
	public int getDate() {
		return date;
	}

	/**
	 * @return the depTime
	 */
	public Long getDepTime() {
		return depTime;
	}

	/**
	 * @return the arrDelayMinutes
	 */
	public Double getArrDelayMinutes() {
		return arrDelayMinutes;
	}
	
	public boolean isCancel(){
		return isCancel;
	}
	
	public boolean isDiverted(){
		return isDiverted;
	}
	
	
	public String toString() {
		return "data: " + this.airlineID + "\t" + this.depTime + "\t" + this.year + "\t" + this.month + "\t" + this.date + "\t" + this.arrDelayMinutes;
	}
}
