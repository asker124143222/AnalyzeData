import org.apache.commons.cli.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/4/25 19:24
 * @Description: 从test-data.txt中导入数据到hbase
 */
public class ImportFromFile {
//    private static String HDFSUri = "hdfs://bigdata-senior01.home.com:9000";
    public static final String NAME = "ImportFromFile";
    //计数器
    public enum Counters {
        LINES
    }

    /**
     * @Author: xu.dm
     * @Date: 2019/4/25 19:26
     * @Description:
     * 输入:文本方式，输出：字节作为键，hbase的Mutation作为输出值
     */
    class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Mutation> {


        private byte[] family = null;
        private byte[] qualifier = null;

        /**
         * Called once at the beginning of the task.
         *
         * @param context
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //从配置文件中读取列族信息，这个信息是控制台方式写入，并通过cli获取
            String column = context.getConfiguration().get("conf.column");
            ColParser parser = new ColParser();
            parser.parse(column);
            if(!parser.isValid()) throw new IOException("family or qualifier error");
            family = parser.getFamily();
            qualifier = parser.getQualifier();
        }

        /**
         * Called once for each key/value pair in the input split. Most applications
         * should override this, but the default is the identity function.
         *
         * @param key
         * @param value
         * @param context
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                //散列每行数据作为行键，根据需求调整
                byte[] rowKey = DigestUtils.md5(line);
                Put put = new Put(rowKey);
                put.addColumn(this.family,this.qualifier, Bytes.toBytes(line));
                context.write(new ImmutableBytesWritable(rowKey),put);
                context.getCounter(Counters.LINES).increment(1);
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        class ColParser {
            private byte[] family;
            private byte[] qualifier;
            private boolean valid;

            public byte[] getFamily() {
                return family;
            }

            public byte[] getQualifier() {
                return qualifier;
            }

            public boolean isValid() {
                return valid;
            }

            public void parse(String value) {
                try {
                    String[] sValue = value.split(":");
                    if (sValue == null || sValue.length < 2 || sValue[0].isEmpty() || sValue[1].isEmpty()) {
                        valid = false;
                        return;
                    }

                    family = Bytes.toBytes(sValue[0]);
                    qualifier = Bytes.toBytes(sValue[1]);
                    valid = true;
                } catch (Exception e) {
                    valid = false;
                }
            }


        }
    }

    private static CommandLine parseArgs(String[] args) throws ParseException{
        Options options = new Options();

        Option option = new Option("t","table",true,"表不能为空");
        option.setArgName("table-name");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("c","column",true,"列族和列名不能为空");
        option.setArgName("family:qualifier");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("i","input",true,"输入文件或者目录");
        option.setArgName("path-in-HDFS");
        option.setRequired(true);
        options.addOption(option);

        options.addOption("d","debug",false,"switch on DEBUG log level");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options,args);
        }catch (Exception e){
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(NAME + " ", options, true);
            System.exit(-1);
        }
        if (cmd.hasOption("d")) {
            Logger log = Logger.getLogger("mapreduce");
            log.setLevel(Level.DEBUG);
        }

        return cmd;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = HBaseConfiguration.create();

        String[] runArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        CommandLine cmd = parseArgs(runArgs);
        if (cmd.hasOption("d")) conf.set("conf.debug", "true");

        String table = cmd.getOptionValue("t");
        String input = cmd.getOptionValue("i");
        String column = cmd.getOptionValue("c");
        //写入配置后，在mapper阶段取出
        conf.set("conf.column", column);

        Job job = Job.getInstance(conf,"Import from file " + input +" into table " + table);
        job.setJarByClass(ImportFromFile.class);
        job.setMapperClass(ImportMapper.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,table);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setNumReduceTasks(0); //不需要reduce

        FileInputFormat.addInputPath(job,new Path(input));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
