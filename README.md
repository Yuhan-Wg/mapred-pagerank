## Implementation of PageRank Algorithm in JAVA/Mapreduce

<img src="interface/img/figure_1.png" width = "300" height = "300" align=left />

#### Dependencies

JAVA 8, hadoop mapreduce

Visualization: 

Python3, matplotlib.pyplot, networkx

#### Dataset

If you want to prefer a small dataset:

* Use [file](input/small_input) as uploaded in this repo.

Elif you prefer a bigger dataset:

* You can try [this: Patent citation network](http://snap.stanford.edu/data/cit-Patents.txt.gz)

The input file contains edges, like:

```
# fromNode \t toNode
1 \t 2
4 \t 1
...
```
The nodes are presented in integer. If you have a file with string nodes, you can index them with [this](src/main/format/ProduceTransitionMatrix.java).

#### MapReduce Tasks Pipeline
**Driver.class**: The main function to run all tasks.

* MapReduce Task1-ProducePageRankOnHDFS: Find unique nodes and initialize PageRank scores with 1.0

* MapReduce Task2-PageRankTransition:
  * TransitionMatrixMapper: Read edges, and output (fromNode, toNode).
  * PageRankStateMapper: Read PageRank scores from previous task, and output (fromNode, PageRank score)
  * MultiplicationReducer: For each fromNode, compute the number of toNodes; Then for each toNode, output (toNode, score/number). Here we finish multiplication computation in the matrix multiplication steps.

* MapReduce Task2-PageRankSum:
  * PageRankMapper: Read output from task2, and multiply beta (for teleports).
  * CompensatoryMapper: Read initial PageRank score or previous PageRank score, and multiply "1-beta" (for teleports).
  * PageRankReducer: For each toNode, add up all middle scores and output (toNode, PageRank score).

* Task2+3 is just one step of transformation. It will repeat over times (provided by input argument).

utils.HadoopParams is where we can set parameters in the tasks.

#### Run the code

In IntelliJ IDEA, run the Driver and input parameters:

```
#args:{inputFilePath, middleResultsPath, finalResultsPath, convergence, beta}
/input/small_input /output/middle_results /output 40 0.2
```

Or run it on your own Hadoop environment.
Start the Hadoop environment and run:

```
# cd <your dir of pagerank project>
$ cd mapred-pagerank

# Upload input files to HDFS
$ hdfs dfs -put ./input /

# Run the tasks
# args:{inputFilePath, middleResultsPath, finalResultsPath, convergence, beta}
$ hadoop jar out/artifacts/mapred_pagerank/mapred-pagerank.jar Driver /input/small_input /output/middle_results /output 40 0.2

# Download result file
$ hdfs dfs -get /output/middle_results/output40/part-r-00000 ./output/pagerank

# Visualization
# args:{pagerank result, edge file, number of most important nodes to show}
$ python3 interface/visualization.py output/pagerank input/small_input/cites.txt 500
```

The visualization result is like:
![](interface/img/figure_1.png)

#### Other applications in this project (not used in the main function).
* **format.ConstructStringIntegerMap**: Build a <String, int> map, each string refers to a unique integer.

* **format.ProduceTransitionMatrix**: Build initial pageRank scores and transition matrix on single machine.

* **pagerank.ProduceTransitionMatrixOnHDFS**: Build transition matrix, used in my first implementation.

* **utils.FuncTools**: Some functions used in this implementation.
  * transToCSV: Transfer results of mapreduce tasks to .csv file.
  * deleteFiles: Delete files.

#### Coding Enviroment

Local Machine: MacOS10.13.6

IDE: IntelliJ IDEA
