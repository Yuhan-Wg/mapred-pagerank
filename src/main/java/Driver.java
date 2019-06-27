import java.io.File;

import pagerank.PageRankSum;
import pagerank.PageRankTransition;
import pagerank.ProduceInitPageRankOnHDFS;
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
        // Initialize pagerank scores with constant(=1.0)
        ProduceInitPageRankOnHDFS.main(args0);

        /* Compute PageRanks
        Loop for "convergence" times:
            for each loop, multiply transition matrix and pagerank
         */
        PageRankTransition pageRankTransition = new PageRankTransition();
        PageRankSum pageRankSum = new PageRankSum();
        for(int i=1;  i<=initializer.convergence;  i++) {
            initializer.update(i);

            /* MapReduce Task 1
            Arguments:
                inputFile:
                    The raw input file, each line is like "fromNode \t toNode" (no whitespace).
                    It will be used as transition matrix: fromNode is column index, toNode is row index.
                prevPageRankPath:
                    Path of the previous pagerank scores, each line is like "node \t pagerankScore"(no whitespace).
                    It will be used as vector: node is row index.
                middleStatePath:
                    Path to save middle output, including pagerank output in each loop.

            pageRankTransition: Multiply each matrix unit with corresponding vector unit.
             */
            String[] args1 = {
                    initializer.inputFile,
                    initializer.prevPageRankPath,
                    initializer.middleStatePath
            };
            pageRankTransition.main(args1);

            /* MapReduce Task 2
            Arguments:
                middleStatePath: Path of middle output.
                prevPageRankPath: Path of pagerank scores from last loop.
                initPageRankPath: Path of initial pagerank scores.
                nextPageRankPath: Path to store pagerank socres in thie loop.
                beta: Parameter to control teleport.

            pageRankSum: Sum up multiplication results from last MapReduce task and add teleport.

            Note: You can choose the teleport type:
                Option A: PR(N) = (1-beta)* TM * PR(N-1) + beta * PR(N-1)
                Option B: PR(N) = (1-beta)* TM * PR(N-1) + beta * PR(0)
                (TM: Transition Matrix; PR(N): PageRank score in Nth loop)
             */
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
            pageRankSum.main(args2);

            // Delete middle results except initial pageRankFile
            // (For easy debug in IntelliJ IDEA)
            //if (i > 1){
            //    FuncTools.deleteFiles(new File(initializer.prevPageRankPath));
            //}
            //FuncTools.deleteFiles(new File(initializer.middleStatePath));
        }

        /*
        Transfer pagerank output to .CSV file for visualization. (Not yet)

        String[] args3 = {
                initializer.inputFile,
                initializer.finalPageRankPath,
                initializer.finalResultsFile
        };
        FuncTools.transToCSV(args3);
        */
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

        // emptyFiles();
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
