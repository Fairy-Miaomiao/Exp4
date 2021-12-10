package exp4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InverseMapper extends Mapper<LongWritable, Text, TextIntWritable, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        TextIntWritable k = new TextIntWritable();
        String[] string = value.toString().split("\t");
        k.set(new Text(string[0]), new IntWritable(Integer.valueOf(string[1])));
        context.write(k, NullWritable.get());

    }
}
