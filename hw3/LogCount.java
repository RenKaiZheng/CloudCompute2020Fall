import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String text = value.toString();

			int indexOpenBracket  = text.indexOf('[');
			int indexCloseBracket = text.indexOf(']');

			if (indexOpenBracket < indexCloseBracket) {
				String timeString = text.substring(indexOpenBracket+1, indexCloseBracket);

				String day = null;
				String month = null;
				String year = null;
				String hour = null;

				int indexSlash = timeString.indexOf('/');
				day = timeString.substring(0, indexSlash);
				timeString = timeString.substring(indexSlash+1, timeString.length);

				indexSlash = timeString.indexOf('/');
				month = timeString.substring(0, indexSlash);
				timeString = timeString.substring(indexSlash+1, timeString.length);
				if (month.equalsIgnoreCase("jan"))
					month = "01";
				else if (month.equalsIgnoreCase("feb"))
					month = "02";
				else if (month.equalsIgnoreCase("mar"))
					month = "03";
				else if (month.equalsIgnoreCase("apr"))
					month = "04";
				else if (month.equalsIgnoreCase("may"))
					month = "05";
				else if (month.equalsIgnoreCase("jun"))
					month = "06";
				else if (month.equalsIgnoreCase("jul"))
					month = "07";
				else if (month.equalsIgnoreCase("aug"))
					month = "08";
				else if (month.equalsIgnoreCase("sep"))
					month = "09";
				else if (month.equalsIgnoreCase("oct"))
					month = "10";
				else if (month.equalsIgnoreCase("nov"))
					month = "11";
				else
					month = "12";

				indexSlash = timeString.indexOf(':');
				year = timeString.substring(0, indexSlash);
				timeString = timeString.substring(indexSlash+1, timeString.length);

				indexSlash = timeString.indexOf(':');
				hour = timeString.substring(0, indexSlash);
				timeString = timeString.substring(indexSlash+1, timeString.length);

				word.set(year+"-"+month+"-"+day+" T "+hour+":00:00.000");
				context.write(word, one);
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(LogCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}