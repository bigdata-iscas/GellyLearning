package com.iscas.SG;

import com.iscas.data.CommunityDetectionData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

/**
 * Created by hadoop on 2/21/17.
 */
public class CommunityDetection {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Edge<Long, Double>> edges = CommunityDetectionData.getSimpleEdgeDataSet(env);
        Graph<Long, Long, Double> graph = Graph.fromDataSet(edges, new MapFunction<Long, Long>() {
            @Override
            public Long map(Long id) throws Exception {
                return id;
            }
        }, env);
        try {
            DataSet<Vertex<Long, Long>> vertices = graph.run(
                    new org.apache.flink.graph.library.CommunityDetection<Long>(1,
                            CommunityDetectionData.DELTA)).getVertices();
            vertices.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
