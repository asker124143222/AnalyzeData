import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/4/26 22:27
 * @Description:
 */
public class AnalyzeData {
    private static final Log LOG = LogFactory.getLog(AnalyzeData.class);

    public static final String NAME = "AnalyzeData";


    /**
     * Parse the command line parameters.
     *
     * @param args The parameters to parse.
     * @return The parsed command line.
     * @throws org.apache.commons.cli.ParseException When the parsing of the parameters fails.
     */
    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        Option o = new Option("t", "table", true,
                "table to read from (must exist)");
        o.setArgName("table-name");
        o.setRequired(true);
        options.addOption(o);
        o = new Option("c", "column", true,
                "column to read data from (must exist)");
        o.setArgName("family:qualifier");
        options.addOption(o);
        o = new Option("o", "output", true,
                "the directory to write to");
        o.setArgName("path-in-HDFS");
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
        String[] runArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        CommandLine cmd = parseArgs(runArgs);
        if(cmd.hasOption("d"))
            conf.set("conf.debug","true");

        String table = cmd.getOptionValue("t");
        String column = cmd.getOptionValue("c");
        String output = cmd.getOptionValue("o");

        ColumnParser columnParser = new ColumnParser();
        columnParser.parse(column);
        if(!columnParser.isValid()) throw new IOException("family or qualifier error");
        byte[] family = columnParser.getFamily();
        byte[] qualifier = columnParser.getQualifier();

        Scan scan = new Scan();
        scan.addColumn(family,qualifier);

        Job job = Job.getInstance(conf,"Analyze data in " + table);
        job.setJarByClass(AnalyzeData.class);
        TableMapReduceUtil.initTableMapperJob(table,scan,AnalyzeMapper.class, Text.class, IntWritable.class,job);
        job.setMapperClass(AnalyzeMapper.class);
        job.setReducerClass(AnalyzeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job,new Path(output));

        System.exit(job.waitForCompletion(true) ? 0:1);

    }


}
