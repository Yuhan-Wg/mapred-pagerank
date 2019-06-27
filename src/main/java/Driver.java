package pagerank;

import java.io.File;
import utils.FuncTools;
import utils.HadoopParams;
/**
 * Created by wangyuhan on 6/17/19.
 */
public class Driver {
    public static void main(String [] args) throws Exception{
        // args:
        //     0: inputFilePath,
        //     1: middleResultsPath,
        //     2: finalResultsPath,
        //     3: convergence,
        //     4: beta
        DriverInitializer initializer = new DriverInitializer(args);

        // Initialize input files
        String[] args0 = {
                initializer.inputFile,
                initializer.initPageRankPath,
                initializer.inputFile
        };
        ProduceInitPageRankOnHDFS.main(args0);
        //ProduceTransitionMatrixOnHDFS.main(args0);

        // Compute PageRanks
        PageRankTransition transitionMultiplication = new PageRankTransition();
        PageRankSum prSum = new PageRankSum();
        for(int i=1;  i<=initializer.convergence;  i++) {
            initializer.update(i);

            // MapReduce Task 1
            String[] args1 = {
                    initializer.inputFile,
                    initializer.prevPageRankPath,
                    initializer.middleStatePath
            };
            transitionMultiplication.main(args1);

            // MapReduce Task 2
            String[] args2;
            if(i==initializer.convergence){
                args2 = new String[]{
                        initializer.middleStatePath,
                        initializer.prevPageRankPath,
                        initializer.nextPageRankPath,
                        initializer.beta
                };
            }else{
                args2 =  new String[]{
                        initializer.middleStatePath,
                        initializer.initPageRankPath,
                        initializer.nextPageRankPath,
                        initializer.beta
                };
            }
            prSum.main(args2);

            // Delete middle results except initial pageRankFile
            if (i > 1){
                FuncTools.deleteFiles(new File(initializer.prevPageRankPath));
            }
            FuncTools.deleteFiles(new File(initializer.middleStatePath));
        }
        String[] args3 = {
                initializer.inputFile,
                initializer.finalPageRankPath,
                initializer.finalResultsFile
        };
        FuncTools.transToCSV(args3);
    }

}

class DriverInitializer{
    // Directories
    public String inputFile;
    public String transitionMatrixPath;
    public String initPageRankPath;
    public String finalPageRankPath;
    public String finalResultsFile;
    // Parameters
    public int convergence;
    public String beta;
    // Middle Parameters
    public String middleResultsPath;
    public String prevPageRankPath;
    public String middleStatePath;
    public String nextPageRankPath;


    public DriverInitializer(String[] args){
        inputFile = args[0];
        middleResultsPath = args[1];
        finalResultsFile = args[2] +"/" +HadoopParams.resultFileName;
        convergence = Integer.parseInt(args[3]);
        beta = args[4].trim();

        transitionMatrixPath = middleResultsPath + "/" + HadoopParams.transitionMatrixDirName;
        initPageRankPath = middleResultsPath + "/"+ HadoopParams.pageRankOutputDirName+0;
        finalPageRankPath = middleResultsPath + "/"+ HadoopParams.pageRankOutputDirName+convergence;

        emptyFiles();
    }

    private void emptyFiles(){
        // Prepare output directories
        // Empty middle result from last the mapred task
        FuncTools.deleteFiles(new File(middleResultsPath));
        FuncTools.deleteFiles(new File(finalResultsFile));
        new File(middleResultsPath).mkdir();
    }

    public void update(int i){
        prevPageRankPath= middleResultsPath + "/"+HadoopParams.pageRankOutputDirName+(i-1);
        middleStatePath = middleResultsPath + "/" + HadoopParams.bufferOutputDirName+i;
        nextPageRankPath = middleResultsPath + "/" + HadoopParams.pageRankOutputDirName+i;
    }
}
