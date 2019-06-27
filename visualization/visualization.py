import networkx as nx
import matplotlib.pyplot as plt
import sys

class GraphVisualization:
    def __init__(self, argv):
        self.pagerank_path = argv[0]
        self.edge_path = argv[1]
        self.num_of_nodes = int(argv[2].strip())

    def main(self):
        self.add_nodes(self.pagerank_path, self.num_of_nodes)
        self.build_graph(self.edge_path)
        self.draw_graph()

    def add_nodes(self,pagerank_path,num_of_nodes):
        node_list = list()
        with open(pagerank_path, "r") as file:
            for line in file.readlines():
                idx, pagerank = int(line.strip().split("\t")[0]), float(line.strip().split("\t")[1])
                node_list.append((idx, pagerank))
        node_list.sort(key = lambda x:x[1], reverse=True)
        if num_of_nodes < len(node_list):
            self.nodes = dict(node_list[:num_of_nodes])
        else:
            self.nodes = dict(node_list)

    def build_graph(self,edge_path):
        G = nx.Graph()
        with open(edge_path, "r") as file:
            for line in file.readlines():
                if len(line)<1 or line[0]=="#" or len(line.strip().split("\t"))<2:
                    continue
                sourceNode, targetNode = int(line.strip().split("\t")[0]),int(line.strip().split("\t")[1])
                if sourceNode in self.nodes and targetNode in self.nodes:
                    G.add_edge(sourceNode,targetNode, weight=(self.nodes[sourceNode] + self.nodes[targetNode])/2)
        self.graph = G

    def draw_graph(self):
        plt.figure(figsize=(8,8))
        pos = nx.spring_layout(self.graph)
        nx.draw_networkx_nodes(self.graph,
         pos=pos,
         node_color='g',
         alpha=1,
         node_size=20
        )
        nx.draw_networkx_edges(self.graph,
         pos=pos,
         edge_color='r',
         alpha=0.2,
         width=1)
        plt.axis('off')
        plt.show()



if __name__ =="__main__":
    args = sys.argv[1:]
    graph_visualization = GraphVisualization(args)
    graph_visualization.main()
