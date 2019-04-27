import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.IOException;


/**
 * @Author: xu.dm
 * @Date: 2019/4/27 13:50
 * @Description:
 */
public class ParseJson {
    private static final String HDFSUri = "hdfs://bigdata-senior01.home.com:9000";
    private static final Log LOG = LogFactory.getLog(ParseJson.class);
    public static final String NAME = "ParseJson";
    public enum Counters {ROWS,COLS,VALID,ERROR};

    static class ParseMapper extends TableMapper<ImmutableBytesWritable, Mutation>{
        private JSONParser parser = new JSONParser();
        private byte[] columnFamily = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            columnFamily = Bytes.toBytes(context.getConfiguration().get("conf.columnFamily"));
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            context.getCounter(Counters.ROWS).increment(1);
            String val = null;
            try {
                Put put = new Put(key.get());
                for(Cell cell : value.listCells()){
                    context.getCounter(Counters.COLS).increment(1);
                    val = Bytes.toStringBinary(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                    JSONObject json = (JSONObject) parser.parse(val);

                    for (Object jsonKey : json.keySet()){
                        Object jsonValue = json.get(jsonKey);
                        put.addColumn(columnFamily,Bytes.toBytes(jsonKey.toString()),Bytes.toBytes(jsonValue.toString()));
                    }
                }
                context.write(key,put);
                context.getCounter(Counters.VALID).increment(1);
            }catch (Exception e){
                e.printStackTrace();
                System.err.println("Error: " + e.getMessage() + ", Row: " +
                        Bytes.toStringBinary(key.get()) + ", JSON: " + value);
                context.getCounter(Counters.ERROR).increment(1);
            }
        }
    }

    private static CommandLine parseArgs(String[] args) throws ParseException{
        Options options = new Options();
        Option o = new Option("i", "input", true,
                "table to read from (must exist)");
        o.setArgName("input-table-name");
        o.setRequired(true);
        options.addOption(o);
        o = new Option("o", "output", true,
                "table to write to (must exist)");
        o.setArgName("output-table-name");
        o.setRequired(true);
        options.addOption(o);
        o = new Option("c", "column", true,
                "column to read data from (must exist)");
        o.setArgName("family:qualifier");
        options.addOption(o);
        options.addOption("d", "debug", false, "switch on DEBUG log level");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(NAME + " ", options, true);
            System.exit(-1);
        }
        if (cmd.hasOption("d")) {
            Logger log = Logger.getLogger("mapreduce");
            log.setLevel(Level.DEBUG);
            System.out.println("DEBUG ON");
        }
        return cmd;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = HBaseConfiguration.create();

//        conf.set("hbase.master","192.168.31.10");
//        conf.set("hbase.zookeeper.quorum", "192.168.31.10");
//        conf.set("hbase.rootdir","hdfs://bigdata-senior01.home.com:9000/hbase");
//        conf.set("hbase.zookeeper.property.dataDir","hdfs://bigdata-senior01.home.com:9000/hbase/zookeeper");

        String[] runArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        CommandLine cmd = parseArgs(runArgs);
        if(cmd.hasOption("d")) conf.set("conf.debug","true");
        String input = cmd.getOptionValue("i");
        String output = cmd.getOptionValue("o");
        String column = cmd.getOptionValue("c");

        ColumnParser columnParser = new ColumnParser();
        columnParser.parse(column);
        if(!columnParser.isValid()) throw new IOException("family or qualifier error");
        byte[] family = columnParser.getFamily();
        byte[] qualifier = columnParser.getQualifier();

        Scan scan = new Scan();
        scan.addColumn(family,qualifier);
        conf.set("conf.columnFamily", Bytes.toStringBinary(family));

        Job job = Job.getInstance(conf, "Parse data in " + input +
                ", write to " + output);
        job.setJarByClass(ParseJson.class);
        TableMapReduceUtil.initTableMapperJob(input,scan,ParseMapper.class,ImmutableBytesWritable.class,Put.class,job);
        TableMapReduceUtil.initTableReducerJob(output, IdentityTableReducer.class,job);

        System.exit(job.waitForCompletion(true)?0:1);

    }

}
