package exp4;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;

public class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
    static int index = 10;
    private Text word = new Text();
    IntWritable one = new IntWritable(1);

   /* @Override
    protected void setup(Context context) throws IOException,
            InterruptedException { //获取缓存文件路径的数组
        Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        //String input=paths[0].toString()+"/train_data.csv";
        //BufferedReader sb = new BufferedReader(new FileReader(input));
        BufferedReader sb = new BufferedReader(new FileReader("input/train_data.csv"));
        //读取BufferedReader里面的数据
        String tmp = null;
        tmp = sb.readLine();
        if (tmp != null) {
            String[] titles = tmp.split(",");
            for (String x : titles) {
                ++index;
                if (x.equals("industry")) {
                    break;
                }
            }
            System.out.println(index);
        }
        sb.close();
    }
*/

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        /*try {
            System.out.println(value);
            String con = value.toString();
            String[] contexts = con.split(",");
            Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
            if (pattern.matcher(contexts[0]).matches()) {
                //System.out.println(contexts[index]);
                word = new Text(contexts[index]);
                //System.out.println(word + "\t" + one);
                context.write(word, one);
            }
        }catch (Exception e){
            e.printStackTrace();
        }*/
        String con = value.toString();
        String[] contexts = con.split(",");
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        if (pattern.matcher(contexts[0]).matches()) {
            //System.out.println(contexts[index]);
            word = new Text(contexts[index]);
            //System.out.println(word + "\t" + one);
            context.write(word, one);
        }
    }

}
