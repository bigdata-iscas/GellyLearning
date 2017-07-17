package com.iscas.SG;

import com.iscas.data.LabelPropagationData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

/**
 * Created by hadoop on 2/22/17.
 */
public class LabelPropagation {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Vertex<Long, Long>> vertices = LabelPropagationData.getDefaultVertexSet(env);
        DataSet<Edge<Long, NullValue>> edges = LabelPropagationData.getDefaultEdgeDataSet(env);
        Graph<Long, Long, NullValue> graph = Graph.fromDataSet(vertices, edges, env);
        try {
            DataSet<Vertex<Long, Long>> result = graph.run(
                    new org.apache.flink.graph.library.LabelPropagation<Long, Long, NullValue>(1));
            result.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
