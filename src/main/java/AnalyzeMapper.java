import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/4/26 21:52
 * @Description:
 */
public class AnalyzeMapper extends TableMapper<Text,IntWritable> {
    private JSONParser parser = new JSONParser();
    public enum Counters { ROWS, COLS, ERROR, VALID }
    private IntWritable ONE = new IntWritable(1);
    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        context.getCounter(Counters.ROWS).increment(1);
        String val = null;
        try {
            for(Cell cell:value.listCells()){
                context.getCounter(Counters.COLS).increment(1);
                val = Bytes.toStringBinary(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                JSONObject json = (JSONObject)parser.parse(val);
                String author = (String)json.get("author");
                if (context.getConfiguration().get("conf.debug") != null)
                    System.out.println("Author: " + author);
                context.write(new Text(author),ONE);
                context.getCounter(Counters.VALID).increment(1);
            }

        }catch (Exception e){
            e.printStackTrace();
            System.err.println("Row: " + Bytes.toStringBinary(key.get()) +
                    ", JSON: " + value);
            context.getCounter(Counters.ERROR).increment(1);
        }

    }
}
