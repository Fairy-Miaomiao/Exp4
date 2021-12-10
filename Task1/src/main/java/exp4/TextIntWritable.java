package exp4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public  class TextIntWritable implements WritableComparable<TextIntWritable> {

    Text word;
    IntWritable count;
    public TextIntWritable(){
        set(new Text(), new IntWritable());
    }
    public void set(Text word, IntWritable count){
        this.word = word;
        this.count = count;
    }
    public void write(DataOutput out) throws IOException {
        word.write(out);
        count.write(out);
    }
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        count.readFields(in);
    }
    @Override
    public String toString(){
        return word.toString() + " " + count.toString();
    }
    @Override
    public int hashCode(){
        return this.word.hashCode() + this.count.hashCode();
    }
    public int compareTo(TextIntWritable o) {
        int result = -1 * this.count.compareTo(o.count);  //先比较次数
        if(result != 0)
            return result;
        return this.word .compareTo(o.word); //次数相同，则按字典排序
    }
}
