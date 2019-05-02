import org.apache.hadoop.hbase.util.Bytes;

/**
 * @Author: xu.dm
 * @Date: 2019/5/2 14:18
 * @Description:
 */
public class BytesSplit {
    public static void main(String[] args) {

        byte[][] splits = Bytes.split(Bytes.toBytes(0), Bytes.toBytes(100), 9);
        int n = 0;
        for (byte[] split : splits) {
            System.out.println("Split key[" + ++n + "]: " +
                    Bytes.toInt(split));
        }

    }
}
