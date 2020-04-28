package flight;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.opencsv.CSVReader;

import temp.RowKeyConverter;

/**
 * @author Shiqi Luo
 *
 */
public class HPopulate {
	
	
	/**
	 * Mapper class to execute map function
	 * @author Shiqi Luo
	 *
	 */
	public static class PopulateMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			CSVReader reader = new CSVReader(new StringReader(value.toString()));
			
			String[] data = null;
			try {
				data = reader.readNext();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try{
				int curYear = Integer.parseInt(data[0]);
				int quarter = Integer.parseInt(data[1]);
				int month = Integer.parseInt(data[2]);
				int dayOfMonth = Integer.parseInt(data[3]);
				int dayOfWeek = Integer.parseInt(data[4]);
				String date = data[5];
				
				String uCarrier = data[6];
				int airlineId = Integer.parseInt(data[7]);
				String carrier = data[8];
				String tailNum = data[9];
				String flightNum = data[10];
				
				String Origin = data[11];
				String OriginCity = data[12];
				String OriginState = data[13];
				String OriginStateFips = data[14];
				String OriginStateName = data[15];
				int OriginWac = Integer.parseInt(data[16]);
                
				String dest = data[17];
				String destCity = data[18];
				String destState = data[19];
				String destStateFips = data[20];
				String destStateName = data[21];
				int destWac = Integer.parseInt(data[22]);
				
				String CRSDepTime = data[23];
				long depTime = Long.parseLong(data[24]);
				double depDelay = Double.parseDouble(data[25]);
				double depDelayMinutes = Double.parseDouble(data[26]);
				double depDel15 = Double.parseDouble(data[27]);
				int departureDelayGroups = Integer.parseInt(data[28]);
				String depTimeBlk = data[29];
				
				double taxiOut = Double.parseDouble(data[30]);
				String wheelsOff = data[31];
				String wheelsOn = data[32];
				double taxiIn = Double.parseDouble(data[33]);
				
				String CRSArrTime = data[34];
				String arrTime = data[35];
				double arrDelay = Double.parseDouble(data[36]);
				double arrDelayMinutes = Double.parseDouble(data[37]);
				double arrDel15 = Double.parseDouble(data[38]);
				int arrivalDelayGroups = Integer.parseInt(data[39]);
				String arrTimeBlk = data[40];
				
				boolean isCancel = (long)Float.parseFloat(data[41]) == 1;
				String cancelCode = data[42];
				boolean isDiverted = (long)Float.parseFloat(data[43]) == 1;
				
				double CRSElapsedTime = Double.parseDouble(data[44]);
				double actualElapsedTime = Double.parseDouble(data[45]);
				double airTime = Double.parseDouble(data[46]);
				double flights = Double.parseDouble(data[47]);
				double distance = Double.parseDouble(data[48]);
				int distanceGroup = Integer.parseInt(data[49]);

				double carrierDelay = Double.parseDouble(data[50]);
				double weatherDelay = Double.parseDouble(data[51]);				
				double NASDelay = Double.parseDouble(data[52]);
				double securityDelay = Double.parseDouble(data[53]);
				double lateAircraftDelay = Double.parseDouble(data[54]);
					
		        byte[] rowKey = RowKeyConverter.makeFlightRowKey(airlineId, curYear,
		        		month, dayOfMonth, depTime);
		        Put put = new Put(rowKey);

		        put.addColumn(Flight.FLIGHT_FAMILY_AIRLINE, Bytes.toBytes("data"), Bytes.toBytes(airlineId));
		        put.addColumn(Flight.FLIGHT_FAMILY_DATE, Bytes.toBytes("year"), Bytes.toBytes(curYear));
		        put.addColumn(Flight.FLIGHT_FAMILY_DATE, Bytes.toBytes("month"), Bytes.toBytes(month));
		        put.addColumn(Flight.FLIGHT_FAMILY_DATE, Bytes.toBytes("date"), Bytes.toBytes(dayOfMonth));
		        put.addColumn(Flight.FLIGHT_FAMILY_ARRDELAY, Bytes.toBytes("data"), Bytes.toBytes(arrDelayMinutes));
		        put.addColumn(Flight.FLIGHT_FAMILY_DEPTIME, Bytes.toBytes("data"), Bytes.toBytes(depTime));
		        put.addColumn(Flight.FLIGHT_FAMILY_ISCANCEL, Bytes.toBytes("data"), Bytes.toBytes(isCancel));
		        put.addColumn(Flight.FLIGHT_FAMILY_ISDIVERTED, Bytes.toBytes("data"), Bytes.toBytes(isDiverted));

		        put.addColumn(Flight.FLIGHT_FAMILY_DATE, Bytes.toBytes("quarter"), Bytes.toBytes(quarter));
		        put.addColumn(Flight.FLIGHT_FAMILY_DATE, Bytes.toBytes("dayofweek"), Bytes.toBytes(dayOfWeek));
		        put.addColumn(Flight.FLIGHT_FAMILY_DATE, Bytes.toBytes("datestr"), Bytes.toBytes(date));
		        
		        put.addColumn(Flight.FLIGHT_FAMILY_INFO, Bytes.toBytes("ucarrier"), Bytes.toBytes(uCarrier));
		        put.addColumn(Flight.FLIGHT_FAMILY_INFO, Bytes.toBytes("carrier"), Bytes.toBytes(carrier));
		        put.addColumn(Flight.FLIGHT_FAMILY_INFO, Bytes.toBytes("tailnum"), Bytes.toBytes(tailNum));
		        put.addColumn(Flight.FLIGHT_FAMILY_INFO, Bytes.toBytes("flightnum"), Bytes.toBytes(flightNum));
		        put.addColumn(Flight.FLIGHT_FAMILY_INFO, Bytes.toBytes("airtime"), Bytes.toBytes(airTime));
		        put.addColumn(Flight.FLIGHT_FAMILY_INFO, Bytes.toBytes("flights"), Bytes.toBytes(flights));
		        
		        put.addColumn(Flight.FLIGHT_FAMILY_ARR, Bytes.toBytes("arrtime"), Bytes.toBytes(arrTime));
		        put.addColumn(Flight.FLIGHT_FAMILY_ARR, Bytes.toBytes("arrdelay"), Bytes.toBytes(arrDelay));
		        put.addColumn(Flight.FLIGHT_FAMILY_ARR, Bytes.toBytes("crsarr"), Bytes.toBytes(CRSArrTime));
		        put.addColumn(Flight.FLIGHT_FAMILY_ARR, Bytes.toBytes("arrdel15"), Bytes.toBytes(arrDel15));
		        put.addColumn(Flight.FLIGHT_FAMILY_ARR, Bytes.toBytes("arrgroup"), Bytes.toBytes(arrivalDelayGroups));
		        put.addColumn(Flight.FLIGHT_FAMILY_ARR, Bytes.toBytes("arrblk"), Bytes.toBytes(arrTimeBlk));
		        
		        put.addColumn(Flight.FLIGHT_FAMILY_ORIGIN, Bytes.toBytes("origin"), Bytes.toBytes(Origin));
		        put.addColumn(Flight.FLIGHT_FAMILY_ORIGIN, Bytes.toBytes("origincity"), Bytes.toBytes(OriginCity));
		        put.addColumn(Flight.FLIGHT_FAMILY_ORIGIN, Bytes.toBytes("originstate"), Bytes.toBytes(OriginState));
		        put.addColumn(Flight.FLIGHT_FAMILY_ORIGIN, Bytes.toBytes("originstatefips"), Bytes.toBytes(OriginStateFips));
		        put.addColumn(Flight.FLIGHT_FAMILY_ORIGIN, Bytes.toBytes("originstatename"), Bytes.toBytes(OriginStateName));
		        put.addColumn(Flight.FLIGHT_FAMILY_ORIGIN, Bytes.toBytes("originwac"), Bytes.toBytes(OriginWac));
		        
		        put.addColumn(Flight.FLIGHT_FAMILY_DEST, Bytes.toBytes("dest"), Bytes.toBytes(dest));
		        put.addColumn(Flight.FLIGHT_FAMILY_DEST, Bytes.toBytes("destcity"), Bytes.toBytes(destCity));
		        put.addColumn(Flight.FLIGHT_FAMILY_DEST, Bytes.toBytes("deststate"), Bytes.toBytes(destState));
		        put.addColumn(Flight.FLIGHT_FAMILY_DEST, Bytes.toBytes("deststatefips"), Bytes.toBytes(destStateFips));
		        put.addColumn(Flight.FLIGHT_FAMILY_DEST, Bytes.toBytes("deststatename"), Bytes.toBytes(destStateName));
		        put.addColumn(Flight.FLIGHT_FAMILY_DEST, Bytes.toBytes("destwac"), Bytes.toBytes(destWac));
		        
		        put.addColumn(Flight.FLIGHT_FAMILY_DEP, Bytes.toBytes("crsdept"), Bytes.toBytes(CRSDepTime));
		        put.addColumn(Flight.FLIGHT_FAMILY_DEP, Bytes.toBytes("deptime"), Bytes.toBytes(depTime));
		        put.addColumn(Flight.FLIGHT_FAMILY_DEP, Bytes.toBytes("depdelay"), Bytes.toBytes(depDelay));
		        put.addColumn(Flight.FLIGHT_FAMILY_DEP, Bytes.toBytes("depdel15"), Bytes.toBytes(depDel15));
		        put.addColumn(Flight.FLIGHT_FAMILY_DEP, Bytes.toBytes("depgroup"), Bytes.toBytes(departureDelayGroups));
		        put.addColumn(Flight.FLIGHT_FAMILY_DEP, Bytes.toBytes("depblk"), Bytes.toBytes(depTimeBlk));
		        put.addColumn(Flight.FLIGHT_FAMILY_DEP, Bytes.toBytes("depdelaymunites"), Bytes.toBytes(depDelayMinutes));
		        
		        put.addColumn(Flight.FLIGHT_FAMILY_OTHER, Bytes.toBytes("taxiout"), Bytes.toBytes(taxiOut));
		        put.addColumn(Flight.FLIGHT_FAMILY_OTHER, Bytes.toBytes("taxiin"), Bytes.toBytes(taxiIn));
		        put.addColumn(Flight.FLIGHT_FAMILY_OTHER, Bytes.toBytes("wheelsoff"), Bytes.toBytes(wheelsOff));
		        put.addColumn(Flight.FLIGHT_FAMILY_OTHER, Bytes.toBytes("wheelson"), Bytes.toBytes(wheelsOn));
		        put.addColumn(Flight.FLIGHT_FAMILY_OTHER, Bytes.toBytes("cancelcode"), Bytes.toBytes(cancelCode));
		        put.addColumn(Flight.FLIGHT_FAMILY_OTHER, Bytes.toBytes("crselapsed"), Bytes.toBytes(CRSElapsedTime));
		        put.addColumn(Flight.FLIGHT_FAMILY_OTHER, Bytes.toBytes("actualelapsed"), Bytes.toBytes(actualElapsedTime));
		        put.addColumn(Flight.FLIGHT_FAMILY_OTHER, Bytes.toBytes("distance"), Bytes.toBytes(distance));
		        put.addColumn(Flight.FLIGHT_FAMILY_OTHER, Bytes.toBytes("distancegroup"), Bytes.toBytes(distanceGroup));
		        
		        put.addColumn(Flight.FLIGHT_FAMILY_DELAYS, Bytes.toBytes("carrierdelay"), Bytes.toBytes(carrierDelay));
		        put.addColumn(Flight.FLIGHT_FAMILY_DELAYS, Bytes.toBytes("weatherdelay"), Bytes.toBytes(weatherDelay));
		        put.addColumn(Flight.FLIGHT_FAMILY_DELAYS, Bytes.toBytes("nasdelay"), Bytes.toBytes(NASDelay));
		        put.addColumn(Flight.FLIGHT_FAMILY_DELAYS, Bytes.toBytes("securitydelay"), Bytes.toBytes(securityDelay));
		        put.addColumn(Flight.FLIGHT_FAMILY_DELAYS, Bytes.toBytes("lateaircraftdelay"), Bytes.toBytes(lateAircraftDelay));
		        
		        
		        context.write(new ImmutableBytesWritable(rowKey), put);

			} catch (NumberFormatException e) {
				e.printStackTrace();	
			}
		}
	}

	/**
	 * Main function of Flight Join class
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new File("/etc/hbase/conf/hbase-site.xml").toURI().toURL());
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		TableName flightTable = TableName.valueOf("Flight");
		
		if(admin.tableExists(flightTable)){
			admin.disableTable(flightTable);
			admin.deleteTable(flightTable);
		}

		TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf("Flight"));
		ArrayList<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_AIRLINE).build());
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_DATE).build());
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_ARRDELAY).build());
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_DEPTIME).build());
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_ISCANCEL).build());
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_ISDIVERTED).build());
		
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_ARR).build());
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_DEP).build());
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_DELAYS).build());
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_DEST).build());
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_OTHER).build());
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_INFO).build());
		descriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Flight.FLIGHT_FAMILY_ORIGIN).build());

		builder.setColumnFamilies(descriptors);
		TableDescriptor tDescriptor = builder.build();
		admin.createTable(tDescriptor);
		admin.close();
		connection.close();

	    Job job = Job.getInstance(conf, "Flight delay compute");
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "Flight");

	    job.setJarByClass(HPopulate.class);
	    job.setMapperClass(PopulateMapper.class);
	    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	    job.setMapOutputValueClass(Put.class);
	    job.setNumReduceTasks(0);
	    job.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    int code = job.waitForCompletion(true) ? 0 : 1;

	    System.exit(code);
	}

}
