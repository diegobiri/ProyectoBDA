import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Reservas {

    public static class ReservasMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text mes = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Asumimos que la línea tiene el formato: reservas_id,fecha
            // Ejemplo: 123,2024-05-21
            String[] fields = line.split(",");
            if (fields.length == 2) {
                String fecha = fields[1];
                String[] fechaParts = fecha.split("-");
                if (fechaParts.length == 3) {
                    String month = fechaParts[1];
                    mes.set(month);
                    context.write(mes, one);
                }
            }
        }
    }

    public static class ReservasReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "reservas por mes");
        job.setJarByClass(Reservas.class);
        job.setMapperClass(ReservasMapper.class);
        job.setCombinerClass(ReservasReducer.class);
        job.setReducerClass(ReservasReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
