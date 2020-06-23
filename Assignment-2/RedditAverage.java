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
import org.json.JSONObject;

public class RedditAverage extends Configured implements Tool {

        public static class RedditMapper
        extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private final static IntWritable one = new IntWritable(1);
                LongPairWritable pair = new LongPairWritable();
				private Text word = new Text();
			
                @Override
                public void map(LongWritable key, Text value, Context context
                                ) throws IOException, InterruptedException {
				
			JSONObject record = new JSONObject(value.toString());
			pair.set(1, (Integer)record.get("score"));
			word.set((String)record.get("subreddit"));
			context.write(word, pair);
                    }
                }


            public static class RedditCombiner
            extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
                private LongPairWritable result = new LongPairWritable();                

                @Override
                public void reduce(Text key, Iterable<LongPairWritable> values,
                                Context context
                                ) throws IOException, InterruptedException {                   
                        int tComment = 0;                    
                        int tScore = 0;
                                                          
                        for (LongPairWritable val : values) {                               
							tComment += (val.get_0());
							tScore += (val.get_1());						
								
                        }
                        
                       result.set(tComment,tScore);
                       context.write(key, result);
                }
        } 
  
        public static class RedditReducer
            extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
                private DoubleWritable result = new DoubleWritable();                

                @Override
                public void reduce(Text key, Iterable<LongPairWritable> values,
                                Context context
                                ) throws IOException, InterruptedException {                   
                        int sum = 0; 
                        double average = 0;
                        int score = 0;
                        //Text word = new Text();                                      
                        for (LongPairWritable val : values) {                               
							sum += (val.get_0());
							score += (val.get_1());						
								
                        }
                        average = ((double)score/sum);
                        result.set(average);
                       context.write(key, result);
                }
        }  

        public static void main(String[] args) throws Exception {
                int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
                System.exit(res);
        }

        @Override
        public int run(String[] args) throws Exception {
                Configuration conf = this.getConf();
                Job job = Job.getInstance(conf, "word count");
                job.setJarByClass(RedditAverage.class);

                job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(RedditMapper.class);
                job.setCombinerClass(RedditCombiner.class);
                job.setReducerClass(RedditReducer.class);

                job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(LongPairWritable.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                TextInputFormat.addInputPath(job, new Path(args[0]));
                TextOutputFormat.setOutputPath(job, new Path(args[1]));

                return job.waitForCompletion(true) ? 0 : 1;
        }
}

