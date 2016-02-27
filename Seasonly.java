import java.io.IOException;
import java.util.*;
import java.lang.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class Seasonly {

	  public static ArrayList<Integer> arrayTemp = new ArrayList<Integer>();
	  public static ArrayList<Integer> arrayPrcp = new ArrayList<Integer>();
	  public static ArrayList<Integer> arrayWind = new ArrayList<Integer>();

	  // Mapper Class
	  public static class MapReduceMapper extends MapReduceBase
	      implements Mapper<LongWritable, Text, Text, Text> {


	    public void map(LongWritable key, Text val,
	        OutputCollector<Text, Text> output, Reporter reporter)
	        throws IOException {

	      // Getting each line of input and converting it into a string
	      String line = val.toString();
	      
	      String getToken[] = line.split(",");
	      String Date = getToken[2];
	      String prcp = getToken[6];
	      String tmp = getToken[3]; 
	      String wind = getToken[13];
	      
	      
	      if(StringUtils.isNumeric(Date)) {
		      String year = Date.substring(0, 4);
		      String month = Date.substring(4,6);
		      String season = "Fall";
		      if (month.equals("12") || month.equals("01") ||month.equals("02"))
		      {
		    	  season = "Winter";
		      }
		      else if (month.equals("03") || month.equals("04") ||month.equals("05"))
		      {
		    	  season = "Spring";
		      }
		      else if (month.equals("06") || month.equals("07") ||month.equals("08"))
		      {
		    	  season = "Summer";
		      }
		      else if (month.equals("09") || month.equals("10") ||month.equals("11"))
		      {
		    	  season = "Fall";
		      }
		      String seasonkey = year + season;
		      String values = tmp + "," + prcp + "," + wind; 
		      
		      output.collect(new Text(seasonkey), new Text(values));
	      }
	    }
	  }


	  // Reducer class
	  public static class MapReduceReducer extends MapReduceBase
	      implements Reducer<Text, Text, Text, Text> {

	    public void reduce(Text key, Iterator<Text> values,
	        OutputCollector<Text, Text> output, Reporter reporter)
	        throws IOException {

	           int temptot = 0, prcptot = 0, windtot=0; 
	           int tempN = 0, prcpN = 0, windN=0; 
	           int avgTemp = 0, avgPrcp = 0,avgWind=0; 
	           float formatTemp, formatPrcp, formatWind; 
	           String compTempRes = "NOT COMPARED", compPrcpRes = "NOT COMPARED", compWindRes = "NOT COMPARED"; 
	           // Summing temperature, precipitation and wind for each year
	           while (values.hasNext()) {
	        	   
	        	   String nextValue = values.next().toString();
	        	   String Climate[] = nextValue.split(",");
	        	   int currTemp = Integer.parseInt(Climate[0]);
	        	   if(currTemp != -9999) {
	        		   temptot += currTemp; 
	        		   tempN += 1; 
	        	   }
	        	   int currPrcp = Integer.parseInt(Climate[1]);
	        	   if(currPrcp != -9999) {
	        		   prcptot += currPrcp; 
	        		   prcpN += 1; 
	        	   }
	        	   int currWind = Integer.parseInt(Climate[2]);        	   
	        	   if(currWind != -9999) {
	        		   windtot += currWind; 
	        		   windN += 1; 
	        	   }
	           }
	          // Finding Average for temperature, Precipitation and Wind
	           if(tempN != 0) {
	        	   avgTemp = temptot / tempN;
	           }
	           if(prcpN != 0) {
	        	   avgPrcp = prcptot / prcpN;
	           }
	           if(windN != 0) {
	        	   avgWind = windtot / windN;
	           }
	           // Next is to gather all the averages and compare with previous year
	           arrayTemp.add(avgTemp);
	           arrayPrcp.add(avgPrcp);
	           arrayWind.add(avgWind);
	           
	           int arrayTempn = arrayTemp.size();
	           int arrayPrcpn = arrayPrcp.size();
	           int arrayWindn = arrayWind.size();
	           // Compare and Record the Comparison result for Temperature, Precipitation and Wind
	           if (arrayTempn > 1) {
	        	   int compInt = arrayTemp.get(arrayTempn - 2);
	        	   if(compInt < avgTemp) {
	        		   compTempRes = "Getting Warmer than Last Season";
	        	   } else if(compInt > avgTemp) {
	        		   compTempRes = "Getting Colder than Last season;";
	        	   } else if(compInt == avgTemp) {
	        		   compTempRes = "Same Result as Last season";
	        	   }
	           }
	           if (arrayPrcpn > 1) {
	        	   int compInt = arrayPrcp.get(arrayPrcpn - 2);
	        	   if(compInt < avgPrcp) {
	        		   compPrcpRes = "Getting Wetter than Last season";
	        	   } else if(compInt > avgPrcp) {
	        		   compPrcpRes = "Getting Dryer than Last season";
	        	   } else if(compInt == avgPrcp) {
	        		   compPrcpRes = "Same Result as Last season";
	        	   }
	           }
	           if (arrayWindn > 1) {
	        	   int compInt = arrayPrcp.get(arrayWindn - 2);
	        	   if(compInt < avgWind) {
	        		   compWindRes = "Getting more windy than Last season";
	        	   } else if(compInt > avgWind) {
	        		   compWindRes = "Getting less windy than Last season";
	        	   } else if(compInt == avgWind) {
	        		   compWindRes = "Same Result as Last season";
	        	   }
	           }
	           
	           // Convert the format from 171 to 17.1
	           formatTemp = (float) (avgTemp * 0.1);
	           formatPrcp = (float) (avgPrcp * 0.1);
	           formatWind = (float) (avgWind);
	           
	           // Sort the Output values into a string
	           String comparison = " ---- " + String.valueOf(formatTemp) + "C -" + compTempRes + 
	        		   " ---- " + String.valueOf(formatPrcp) + "mm -" + compPrcpRes + 
	        		   " ---- " + String.valueOf(formatWind) + "miles/hr - " + compWindRes;
	          
	           output.collect(key, new Text(comparison));  // sending the value to output file
	    }
	  }



	  
	  public static void main(String[] args) {

	    JobConf conf = new JobConf(Seasonly.class);
	    conf.setJobName("Seasonly");
	    
	    conf.setNumMapTasks(Integer.parseInt(args[2]));
	    conf.setNumReduceTasks(Integer.parseInt(args[3]));
	    
	    long jobStartTime = System.currentTimeMillis();

	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);
	  
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    
	    FileInputFormat.addInputPath(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	    conf.setMapperClass(MapReduceMapper.class);
	    conf.setReducerClass(MapReduceReducer.class);

	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	    
	    long jobEndTime = System.currentTimeMillis();
	    
	    // Calculating the Time Taken by the job
	    long timeTaken = jobEndTime - jobStartTime;
	    System.out.println("\n Time Taken = " + timeTaken);
	    
	  }

}
