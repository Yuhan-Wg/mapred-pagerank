package HadoopUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.io.File;
/**
 * Created by wangyuhan on 6/17/19.
 */
public class FuncTools {
    public static void transToCSV(String[] args) throws IOException{
        BufferedReader transition = new BufferedReader(new FileReader(args[0] + "/part-r-00000"));
        BufferedReader pr = new BufferedReader(new FileReader(args[1] + "/part-r-00000"));
        FileWriter fileWriter = new FileWriter(args[2]);

        Map<String, String> page_pr = new HashMap<String, String>();

        String line = pr.readLine();
        while (line != null) {
            page_pr.put(
                    line.split(HadoopParams.SPARATOR)[0],
                    line.split(HadoopParams.SPARATOR)[1]);
            line = pr.readLine();
        }
        pr.close();

        line = transition.readLine();
        fileWriter.write("source,target,value\n");

        while (line != null) {

            String[] from_tos = line.split(HadoopParams.SPARATOR);
            String[] tos = from_tos[1].split(HadoopParams.subSPARATOR);
            for (String to: tos) {
                String value = page_pr.get(to);
                fileWriter.write(from_tos[0] + "," + to + "," + value + "\n");
            }
            line = transition.readLine();
        }

        transition.close();
        fileWriter.close();
    }

    public static void deleteFiles(File file){
        if(!file.exists()) return;

        if(file.isFile() || file.list()==null){
            file.delete();
            return;
        }else{
            File[] files = file.listFiles();
            for(File f:files){
                deleteFiles(f);
            }
            file.delete();
            return;
        }

    }
}
