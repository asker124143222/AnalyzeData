import org.apache.hadoop.hbase.util.Bytes;

/**
 * @Author: xu.dm
 * @Date: 2019/4/26 22:51
 * @Description:
 */
public class ColumnParser {
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
