package com.iscas;

import com.iscas.data.TriangleCountData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

/**
 * Created by hadoop on 2/22/17.
 */
public class TriangleEnumerator {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Graph<Long, NullValue, NullValue> graph = Graph.fromDataSet(
                TriangleCountData.getDefaultEdgeDataSet(env), env);
        try {
            DataSet<Tuple3<Long, Long, Long>> triangles = graph.run(
                    new org.apache.flink.graph.library.TriangleEnumerator<Long, NullValue, NullValue>());
            triangles.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
