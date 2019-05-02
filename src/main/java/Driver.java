import org.apache.hadoop.util.ProgramDriver;

/**
 * @Author: xu.dm
 * @Date: 2019/5/2 15:06
 * @Description: 使用ProgramDriver驱动jar入口，
 * 例如：进入到ParseJson的jar入口：hadoop jar myDriver.jar ParseJson -i importTable -o testtable2 -c data:json
 * 进入AnalyzeData的jar入口：hadoop jar myDriver.jar AnalyzeData -t importTable -c data:json -o /output10
 */
public class Driver {
    public static void main(String[] args) throws Throwable {
        ProgramDriver pgd = new ProgramDriver();
        pgd.addClass(ImportFromFile.NAME, ImportFromFile.class, "ImportFromFile");
        pgd.addClass(AnalyzeData.NAME, AnalyzeData.class, "AnalyzeData");
        pgd.addClass(ParseJson.NAME, ParseJson.class, "ParseJson");
        pgd.addClass(ParseJsonMulti.NAME, ParseJsonMulti.class, "Parse JSON into multiple tables");
        pgd.driver(args);
    }
}
