import java.io.File;
/**
 * Created by wangyuhan on 6/17/19.
 */
public class Driver {
    public static void main(String [] args) throws Exception{
        //args0: dir of transition.txt
        //args1: dir of initPageRank.txt
        //args2: dir of middleResults
        //args3: dir of finalResults
        //args4: times of convergence

        PageRankTransition transitionMultiplication = new PageRankTransition();
        PageRankSum prSum = new PageRankSum();

        String transitionMatrixPath = args[0];
        String initPageRankStatePath = args[1];
        String middleResultsPath = args[2];
        String finalResultsPath = args[3];
        int convergence = Integer.parseInt(args[4]);


        for(int i=0;  i<convergence;  i++) {
            String pageRankPath;
            if(i==0){
                pageRankPath = initPageRankStatePath;
            }else {
                pageRankPath = middleResultsPath + "/output"+i;
            }

            String middleStatePath = middleResultsPath + "/buffer"+i;
            String nextPageRankPath = middleResultsPath + "/output"+(i+1);

            // MapReduce Task 1
            String[] args1 = {transitionMatrixPath, pageRankPath, middleStatePath};
            deleteFiles(new File(middleStatePath));
            transitionMultiplication.main(args1);

            // MapReduce Task 2
            String[] args2 = {middleStatePath, nextPageRankPath};
            deleteFiles(new File(nextPageRankPath));
            prSum.main(args2);

        }
        String pageRankPath = middleResultsPath + "/output"+convergence;
        String[] args3 = {transitionMatrixPath, pageRankPath, finalResultsPath};
        deleteFiles(new File(finalResultsPath + "/result.csv"));
        TransferToCSV.main(args3);
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
