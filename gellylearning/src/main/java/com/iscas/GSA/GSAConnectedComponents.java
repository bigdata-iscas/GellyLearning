package com.iscas.GSA;

import com.iscas.data.ConnectedComponentsDefaultData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

/**
 * Created by hadoop on 2/21/17.
 */
public class GSAConnectedComponents {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Edge<Long, NullValue>> edges = ConnectedComponentsDefaultData.getDefaultEdgeDataSet(env);
        Graph<Long, Long, NullValue> graph = Graph.fromDataSet(edges, new MapFunction<Long, Long>() {
            @Override
            public Long map(Long id) throws Exception {
                return id;
            }
        }, env);
        try {
            DataSet<Vertex<Long, Long>> vertexWithMinId =
                    graph.run(new org.apache.flink.graph.library.GSAConnectedComponents<Long, Long, NullValue>(4));
            vertexWithMinId.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
