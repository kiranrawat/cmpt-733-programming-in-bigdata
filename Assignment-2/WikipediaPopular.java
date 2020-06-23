import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WikipediaPopular extends Configured implements Tool {

        public static class WikiMapper
        extends Mapper<LongWritable, Text, Text, LongWritable>{

                private Text time = new Text();
                LongWritable visited = new LongWritable();
            
                @Override
                public void map(LongWritable key, Text value, Context context
                                ) throws IOException, InterruptedException {
                String[] data = value.toString().split("\\s+");  
                        if(data[1].equals("en")) {    
                            if(!(data[2].equals("Main_Page")) && !(data[2].startsWith("Special:"))) {                                    
                                time.set(data[0]);
                                visited = new LongWritable(new Long(data[3]));
                                context.write(time,visited);
                             }                           
                        }
                }
        }
  
        public static class WikiReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {
               LongWritable max = new LongWritable(); 
              // LongWritable max = null;             

                @Override
                public void reduce(Text key, Iterable<LongWritable> values,
                                Context context
                                ) throws IOException, InterruptedException {                   
                          
                        for (LongWritable val : values) { 
                            if( max.compareTo(val) < 0)  {
                              max.set(new Long(val.toString()));
                            }   
                        }
                        context.write(key, max);
                }
        }  

        public static void main(String[] args) throws Exception {
                int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
                System.exit(res);
        }

        @Override
        public int run(String[] args) throws Exception {
                Configuration conf = this.getConf();
                Job job = Job.getInstance(conf, "WikipediaPopular");
                job.setJarByClass(WikipediaPopular.class);

                job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(WikiMapper.class);
                job.setCombinerClass(WikiReducer.class);
                job.setReducerClass(WikiReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                TextInputFormat.addInputPath(job, new Path(args[0]));
                TextOutputFormat.setOutputPath(job, new Path(args[1]));

                return job.waitForCompletion(true) ? 0 : 1;
        }
}

