package exp4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Runner {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //对input文件夹下的每个文件分别进行WordCount,结果保存在
        Path tempDir = new Path("industry-count-temp-output");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        if (fs.exists(tempDir)) {
            fs.delete(tempDir, true);
        }

        //System.out.println("1");
        Job job = new Job(conf, "Count");
        job.addCacheFile(new URI(args[0]));
        job.setJarByClass(Runner.class);
        job.setMapperClass(CountMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //List<String> other_args = new ArrayList<String>();

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, tempDir);
        job.waitForCompletion(true);


        System.out.println("还有排序没做");
        /*Job sortjob = new Job(conf, "sort");
        FileInputFormat.addInputPath(sortjob, tempDir);
        sortjob.setInputFormatClass(SequenceFileInputFormat.class);
        sortjob.setMapperClass(InverseMapper.class);
        sortjob.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(sortjob, new Path(args[1]));
        sortjob.setOutputKeyClass(IntWritable.class);
        sortjob.setOutputValueClass(Text.class);
        sortjob.setSortComparatorClass(CountComparator.class);

        sortjob.waitForCompletion(true);*/

        Job sortjob = new Job(conf, "sort");
        FileInputFormat.addInputPath(sortjob, tempDir);
        //sortjob.setInputFormatClass(SequenceFileInputFormat.class);
        sortjob.setMapperClass(InverseMapper.class);
        sortjob.setReducerClass(InverseReducer.class);
        sortjob.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(sortjob, new Path(args[1]));
        sortjob.setOutputKeyClass(TextIntWritable.class);
        sortjob.setOutputValueClass(NullWritable.class);
        //sortjob.setSortComparatorClass(CountComparator.class);

        sortjob.waitForCompletion(true);


    }
}

