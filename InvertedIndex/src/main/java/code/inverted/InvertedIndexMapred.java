package code.inverted;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mapreduce.LemmaIndexMapred.LemmaIndexMapper;
import mapreduce.util.IndexableFileInputFormat;
import mapreduce.util.StringIntegerList;
import mapreduce.util.StringIntegerList.StringInteger;
import mapreduce.util.WikipediaPageInputFormat;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred {
	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, StringInteger> {

		@Override
		public void map(Text articleTitle, Text indices, Context context) throws IOException,
				InterruptedException {
			// TODO: You should implement inverted index mapper here
			StringIntegerList indincesList=new StringIntegerList();
			System.out.print(articleTitle.toString());
			System.out.println("~~~~~~");
			System.out.print(indices.toString());
			indincesList.readFromString(indices.toString());			
			List<StringInteger> outputList=indincesList.getIndices();
			StringInteger Value;
			Text Key=new Text();	
			for (StringInteger pair : outputList) {  
				Key.set(pair.getString());
				Value=new StringInteger(articleTitle.toString(),pair.getValue());
//				System.out.println(pair.getString()+"~"+pair.getValue());
				context.write(Key,Value);			  
			} 
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, StringInteger, Text, StringIntegerList> {
		@Override
		public void reduce(Text lemma, Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement inverted index reducer here
			
			Text Key=lemma;
			StringIntegerList  Value;
			HashMap<String,Integer> stringIntegerMap=new HashMap<String,Integer>();
			for(StringInteger stringInteger:articlesAndFreqs){
//				System.out.println(stringInteger.getString()+"~"+stringInteger.getValue());
				stringIntegerMap.put(stringInteger.getString(),new Integer(stringInteger.getValue()));
			}		
			Value=new StringIntegerList(stringIntegerMap);
			context.write(Key,Value);			
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO: you should implement the Job Configuration and Job call
		// here
		Configuration conf=new Configuration();
		GenericOptionsParser gop=new GenericOptionsParser(conf, args);
		String[] otherArgs=gop.getRemainingArgs();
		
		if(otherArgs[0].equals("-l")){
			Job job=Job.getInstance(conf, "Lamma Index");
			job.setJarByClass(InvertedIndexMapred.class);
			job.setNumReduceTasks(0);
			job.setMapperClass(LemmaIndexMapper.class);	
			job.setInputFormatClass(WikipediaPageInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(StringIntegerList.class);
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
			
			System.exit(job.waitForCompletion(true)?0:1);

		}
		else if(otherArgs[0].equals("-i")){
			conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "	"); 
			Job job=Job.getInstance(conf, "Inverted Index");
			job.setJarByClass(InvertedIndexMapred.class);
			job.setMapperClass(InvertedIndexMapper.class);
			job.setReducerClass(InvertedIndexReducer.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(StringInteger.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(StringIntegerList.class);
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
			
			System.exit(job.waitForCompletion(true)?0:1);
		}
	
	}
}
