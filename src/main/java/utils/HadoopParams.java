package HadoopUtils;

import java.text.DecimalFormat;
import java.util.regex.Pattern;

/**
 * Created by wangyuhan on 6/25/19.
 */
public class HadoopParams {

    public static final String SPARATOR = "\t";

    public static final String subSPARATOR = ",";

    public static final String bufferOutputDirName = "buffer";

    public static final String pageRankOutputDirName = "output";

    public static final String transitionMatrixDirName = "tm";

    public static final String resultFileName = "result.csv";

    public static final DecimalFormat decimalFormat = new DecimalFormat("#.0000000");

    public static final String skipSign = "#";

}