import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/5/1 20:33
 * @Description: 把hbase数据表里的json分解，键值为link的存储到linktable，其他存储到infotable
 */
public class ParseJsonMulti {
    private static final Log LOG = LogFactory.getLog(ParseJsonMulti.class);

    public static final String NAME = "ParseJsonMulti";
    public enum Counters {ROWS,COLS,VALID,ERROR}

    static class ParseMapper extends TableMapper<ImmutableBytesWritable, Writable>{
        private Connection connection =null;
        private BufferedMutator infoTable = null;
        private BufferedMutator linkTable = null;
        private JSONParser parser = new JSONParser();
        private byte[] columnFamily = null;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            connection = ConnectionFactory.createConnection(context.getConfiguration());
            infoTable = connection.getBufferedMutator(TableName.valueOf(context.getConfiguration().get("conf.infotable")));
            linkTable = connection.getBufferedMutator(TableName.valueOf(context.getConfiguration().get("conf.linktable")));
            columnFamily = Bytes.toBytes(context.getConfiguration().get("conf.columnfamily"));
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            context.getCounter(Counters.ROWS).increment(1);
            String columnValue =null;
            try {
                Put infoPut = new Put(key.get());
                Put linkPut = new Put(key.get());
                for(Cell cell : value.listCells()){
                    context.getCounter(Counters.COLS).increment(1);
                    columnValue = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                    JSONObject json = (JSONObject)parser.parse(columnValue);
                    for(Object jsonKey : json.keySet()){
                        Object jsonValue = json.get(jsonKey);
                        if ("link".equals(jsonKey)){
                            linkPut.addColumn(columnFamily,Bytes.toBytes(jsonKey.toString()),Bytes.toBytes(jsonValue.toString()));
                        }else{
                            infoPut.addColumn(columnFamily,Bytes.toBytes(jsonKey.toString()),Bytes.toBytes(jsonValue.toString()));
                        }
                    }
                }
                infoTable.mutate(infoPut);
                linkTable.mutate(linkPut);
                context.getCounter(Counters.VALID).increment(1);
            }catch (Exception e){
                e.printStackTrace();
                System.err.println("ERROR: "+e.getMessage()+",Row: "+Bytes.toStringBinary(key.get())+", JSON: "+columnValue);
                context.getCounter(Counters.ERROR).increment(1);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            infoTable.flush();
            linkTable.flush();
        }
    }

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        Option o = new Option("i", "input", true,
                "table to read from (must exist)");
        o.setArgName("input-table-name");
        o.setRequired(true);
        options.addOption(o);
        o = new Option("c", "column", true,
                "column to read data from (must exist)");
        o.setArgName("family:qualifier");
        options.addOption(o);
        o = new Option("o", "infotbl", true,
                "info table to write to (must exist)");
        o.setArgName("info-table-name");
        o.setRequired(true);
        options.addOption(o);
        o = new Option("l", "linktbl", true,
                "link table to write to (must exist)");
        o.setArgName("link-table-name");
        o.setRequired(true);
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
        String[] runArgs = new GenericOptionsParser(args).getRemainingArgs();
        CommandLine cmd = parseArgs(runArgs);

        if(cmd.hasOption("d")) conf.set("conf.debug","true");
        String input = cmd.getOptionValue("i");
        String column = cmd.getOptionValue("c");

        conf.set("conf.infotable", cmd.getOptionValue("o"));
        conf.set("conf.linktable", cmd.getOptionValue("l"));

        ColumnParser columnParser = new ColumnParser();
        columnParser.parse(column);
        if(!columnParser.isValid()) throw new IOException("family or qualifier error");
        byte[] family = columnParser.getFamily();
        byte[] qualifier = columnParser.getQualifier();
        conf.set("conf.columnfamily", Bytes.toStringBinary(family));
        conf.set("conf.columnqualifier",Bytes.toStringBinary(qualifier));

        Scan scan = new Scan();
        scan.addColumn(family,qualifier);

        Job job = Job.getInstance(conf,ParseJsonMulti.NAME);
        job.setJarByClass(ParseJsonMulti.class);

        TableMapReduceUtil.initTableMapperJob(input,scan,ParseMapper.class,ImmutableBytesWritable.class,Put.class,job);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
