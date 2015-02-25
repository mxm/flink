package org.apache.flink.test.convenience;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

import static org.junit.Assert.*;

import org.apache.flink.test.iterative.nephele.danglingpagerank.BooleanValue;
import org.junit.Test;


public class CountCollectITCase {

    @Test
    public void testSimple() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setDegreeOfParallelism(5);
        
        Integer[] input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        
        DataSet<Integer> data = env.fromElements(input);

        // count
        long numEntries = data.count();
        assertEquals(10, numEntries);

        // collect
        ArrayList<Integer> list = (ArrayList<Integer>) data.collect();
        assertArrayEquals(input, list.toArray());

    }
    
    @Test
    public void testAdvanced() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setDegreeOfParallelism(5);
        env.getConfig().disableObjectReuse();


        DataSet<Integer> data = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        DataSet<Integer> data2 = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        DataSet<Tuple2<Integer, Integer>> data3 = data.cross(data2);

        // count
        long numEntries = data3.count();
        assertEquals(100, numEntries);

        // collect
        ArrayList<Tuple2<Integer, Integer>> list = (ArrayList<Tuple2<Integer, Integer>>) data3.collect();
        System.out.println(list);

        // set expected entries in a hash map to true
        HashMap<Tuple2<Integer, Integer>, Boolean> expected = new HashMap<Tuple2<Integer, Integer>, Boolean>();
        for (int i = 1; i <= 10; i++) {
            for (int j = 1; j <= 10; j++) {
                expected.put(new Tuple2<Integer, Integer>(i, j), true);
            }
        }

        // check if all entries are contained in the hash map
        for (int i = 0; i < 100; i++) {
            Tuple2<Integer, Integer> element = list.get(i);
            assertEquals(expected.get(element), true);
            expected.remove(element);
        }

    }
}
