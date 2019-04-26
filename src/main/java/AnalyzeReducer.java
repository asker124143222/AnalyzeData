import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/4/26 22:25
 * @Description:
 */
public class AnalyzeReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     *
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable one:values) count++;

        if (context.getConfiguration().get("conf.debug") != null)
            System.out.println("Author: " + key.toString() + ", Count: " + count);

        context.write(key,new IntWritable(count));
    }
}
