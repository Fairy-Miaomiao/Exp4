package exp4;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InverseReducer extends Reducer<TextIntWritable, NullWritable, TextIntWritable, NullWritable> {
    public void reduce(TextIntWritable key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException{
        for(NullWritable v : value)
            context.write(key, v);
    }
}
