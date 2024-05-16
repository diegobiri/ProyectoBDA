import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Reservas 
{
	public class UserAmountMapper extends Mapper<LongWritable, Integer, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fecha = line[1].split(",");
			int reservas_id = line[0];
			String mes =  fecha[1];
			context.write(new Text(reservas_id), new Text(mes));
			}
		}
	}

	public class UserAmountReducer extends Reducer<Integer, Text> {

		@Override
		public void reduce(Integer key, Text<Text> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			for (Integer val : values) {
				sum += val.get();
			}
			context.write(key, new Integer(sum));
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "user amount spent");
		job.setJarByClass(Reservas.class);
		job.setMapperClass(ReservasMapper.class);
		job.setCombinerClass(ReservasReducer.class);
		job.setReducerClass(ReservasReducer.class);
		job.setOutputKeyClass(Integer.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}