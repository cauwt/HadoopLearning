package hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.Objects;

/**
 * Created by yachao on 17/9/24.
 */
public class RowNumUDF extends UDF {

    public static String signature = "_";
    public static int order = 0;

    public int evaluate(Text text) {
        if (Objects.nonNull(text)) {
            String colName = text.toString();

            if (signature.equals("_")) {
                signature = colName;
                order = 1;

                return order;
            } else {
                if (signature.equals(colName)) {
                    order += 1;
                    return order;
                } else {
                    signature = colName;
                    order = 1;
                    return order;
                }
            }
        } else {
            return -1;
        }
    }

}
