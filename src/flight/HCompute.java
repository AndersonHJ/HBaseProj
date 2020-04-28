package flight;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author Shiqi Luo
 *
 */
public class HCompute {

	public static class AirlinePartitioner extends Partitioner<IntPair, NumberPair> {

	    @Override
	    public int getPartition(IntPair key, NumberPair value, int numPartitions) {
	    	return (key.getFirst() % 10);
	    }
	}
	
	/**
	 * Mapper class to execute map function
	 * @author Shiqi Luo
	 *
	 */
	public static class ComputeMapper extends TableMapper<IntPair, NumberPair> {

		private IntPair airline = null;

		public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			int airlineId = Bytes.toInt(value.getValue(Flight.FLIGHT_FAMILY_AIRLINE, Bytes.toBytes("data")));
			int month = Bytes.toInt(value.getValue(Flight.FLIGHT_FAMILY_DATE, Bytes.toBytes("month")));
			double delay = Bytes.toDouble(value.getValue(Flight.FLIGHT_FAMILY_ARRDELAY, Bytes.toBytes("data")));
			
			this.airline = new IntPair(airlineId, month);
			
			context.write(this.airline, new NumberPair(delay, 1));
		}
	}
	
	
	public static class ComputeCombiner extends Reducer<IntPair, NumberPair, IntPair, NumberPair> {
		
		public void reduce(IntPair key, Iterable<NumberPair> values, Context context) throws IOException, InterruptedException {

			 double delaySum = 0;
			 int count = 0;

			 for(NumberPair delayValue: values){
				 delaySum += delayValue.getFirst();
				 count += delayValue.getSecond();
			 }

			 context.write(new IntPair(key.getFirst(), key.getSecond()), new NumberPair(delaySum, count));
		}
	}
	
	/**
	 * Reducer class to execute reducer function
	 * @author Shiqi Luo
	 *
	 */
	public static class FlightsDelayReducer extends Reducer<IntPair, NumberPair, IntWritable, Text> {
		
		 public void reduce(IntPair key, Iterable<NumberPair> values, Context context) throws IOException, InterruptedException {
			 
			 int month = 1;
			 int airlineId = key.getFirst();
			 double delaySum = 0;
			 int count = 0;
			 StringBuilder list = new StringBuilder();
			 ArrayList<Integer> months = new ArrayList<>();
			 ArrayList<Double> delays = new ArrayList<>();
			 ArrayList<Integer> counts = new ArrayList<>();
			 
			 for(NumberPair delayValue: values){
				 months.add(key.getSecond());
				 delays.add(delayValue.getFirst());
				 counts.add(delayValue.getSecond());
			 }

			 int index = 0;
			 while(month<13){
				 if(index < months.size() && month == months.get(index)) {
					 delaySum += delays.get(index);
					 count += counts.get(index);
					 index++;
				 }
				 else {
					 if(count == 0){
						 list.append("(" + month + ", 0), ");
					 }
					 else{
						 list.append("(" + month + ", " + (int)Math.round(delaySum/count) + "), ");
					 }
					 month++;
					 delaySum = 0;
					 count = 0;
				 }
			 }
			 
			 context.write(new IntWritable(airlineId), new Text(list.toString()));
		}
	}
	
	public static class KeyComparator extends WritableComparator {
	    protected KeyComparator() {
	      super(IntPair.class, true);
	    }
	    
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	      IntPair ip1 = (IntPair) w1;
	      IntPair ip2 = (IntPair) w2;
	      return ip1.compareTo(ip2);
	    }
	}
	  
	public static class GroupComparator extends WritableComparator {
	    protected GroupComparator() {
	      super(IntPair.class, true);
	    }
	    
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	      IntPair ip1 = (IntPair) w1;
	      IntPair ip2 = (IntPair) w2;
	      return Integer.compare(ip1.getFirst(), ip2.getFirst());
	    }
	}

	/**
	 * Main function of Flight Join class
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new File("/etc/hbase/conf/hbase-site.xml").toURI().toURL());
		
	    Job job = Job.getInstance(conf, "Flight delay compute");
	    job.getConfiguration().set(TableInputFormat.INPUT_TABLE, "Flight");
	    job.setJarByClass(HCompute.class);
	    job.setMapperClass(ComputeMapper.class);
	    job.setCombinerClass(ComputeCombiner.class);
	    
	    job.setReducerClass(FlightsDelayReducer.class);
	    job.setPartitionerClass(AirlinePartitioner.class);
	    job.setSortComparatorClass(KeyComparator.class);
	    job.setGroupingComparatorClass(GroupComparator.class);
	    
	    job.setMapOutputKeyClass(IntPair.class);
	    job.setMapOutputValueClass(NumberPair.class);
	    job.setNumReduceTasks(10);
	    
	    Scan scan = new Scan();
	    scan.setCaching(500);
	    scan.setCacheBlocks(false);
	    scan.addColumn(Flight.FLIGHT_FAMILY_AIRLINE, Bytes.toBytes("data"));
	    scan.addColumn(Flight.FLIGHT_FAMILY_DATE, Bytes.toBytes("year"));
	    scan.addColumn(Flight.FLIGHT_FAMILY_DATE, Bytes.toBytes("month"));
	    scan.addColumn(Flight.FLIGHT_FAMILY_ARRDELAY, Bytes.toBytes("data"));
	    scan.addColumn(Flight.FLIGHT_FAMILY_ISCANCEL, Bytes.toBytes("month"));
	    scan.addColumn(Flight.FLIGHT_FAMILY_ISDIVERTED, Bytes.toBytes("data"));
	    
	    Filter isCancelFilter = new SingleColumnValueExcludeFilter(Flight.FLIGHT_FAMILY_ISCANCEL, 
	    		Bytes.toBytes("data"), CompareOperator.EQUAL, Bytes.toBytes(true));
	    Filter isDivertedFilter = new SingleColumnValueExcludeFilter(Flight.FLIGHT_FAMILY_ISCANCEL, 
	    		Bytes.toBytes("data"), CompareOperator.EQUAL, Bytes.toBytes(true));
	    Filter yearFilter = new SingleColumnValueFilter(Flight.FLIGHT_FAMILY_DATE, 
	    		Bytes.toBytes("year"), CompareOperator.EQUAL, Bytes.toBytes(Integer.parseInt(args[1])));
	    
	    List<Filter> filters = Arrays.asList(isCancelFilter, isDivertedFilter, yearFilter);
	    scan.setFilter(new FilterList(filters));
	    
	    TableMapReduceUtil.initTableMapperJob("Flight", scan, ComputeMapper.class, IntPair.class, NumberPair.class, job);
	    FileOutputFormat.setOutputPath(job, new Path(args[0]));
	    
	    int code = job.waitForCompletion(true) ? 0 : 1;

	    System.exit(code);
	}
}
