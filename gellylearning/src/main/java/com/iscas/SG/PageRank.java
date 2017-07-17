package com.iscas.SG;

import com.iscas.data.PageRankData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

/**
 * Created by hadoop on 2/20/17.
 */
public class PageRank {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Edge<Long, Double>> edges = PageRankData.getDefaultEdgeDataSet(env);
        Graph<Long, Double, Double> graph = Graph.fromDataSet(edges, new InitVertice(), env);
        try {
            DataSet<Vertex<Long, Double>> result = new org.apache.flink.graph.library.PageRank<Long>(0.85, 3).run(graph);
            result.print();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public final static class InitVertice implements MapFunction<Long, Double> {
        @Override
        public Double map(Long id) throws Exception {
            return Double.POSITIVE_INFINITY;
        }
    }
}
