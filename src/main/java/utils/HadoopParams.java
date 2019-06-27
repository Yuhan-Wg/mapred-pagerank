package utils;

import java.text.DecimalFormat;

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

    public static final float initialPageRank = 1f;

    public static int hashCode(String str){
        return str.hashCode();
    }

    public static String[] mapLine(String line){
        if(line.startsWith(HadoopParams.skipSign)){
            return new String[]{};
        }
        return line.trim().split(HadoopParams.SPARATOR);
    }



}
