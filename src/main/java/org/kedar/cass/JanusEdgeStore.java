package org.kedar.cass;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by kedar on 5/17/17.
 */
public class JanusEdgeStore {
    public static void main(String[] args) throws Exception {
        String serverIP = "127.0.0.1";
        String keyspace = "janusgraph";
        String table = "edgestore";

        Cluster cluster = Cluster.builder()
            .addContactPoints(serverIP)
            .build();

        Session session = cluster.connect(keyspace);
        String cqlStatement = "SELECT * FROM " + table;
//        Multimap<Integer, Pair<ByteBuffer, ByteBuffer>> mmap = ArrayListMultimap.create();
        Set<Integer> vertHashcodes = new HashSet<>();
        Set<ByteBuffer> vertByteBuffers = new HashSet<>();
        for (ColumnMetadata meta : cluster.getMetadata().getKeyspace(keyspace).getTable(table).getPartitionKey()) {
            System.out.println("meta: " + meta.getName());
        }
        int rowId = 0;
        for (Row row : session.execute(cqlStatement)) {
            rowId++;
//            mmap.put(row.getBytes(0).hashCode(), new Pair<>(row.getBytes(1), row.getBytes(2)));
            System.out.println("column definitions size: " + row.getColumnDefinitions().size());
            for (ColumnDefinitions.Definition d : row.getColumnDefinitions()) {
                System.out.println(d.getName());
            }
            byte[] array = row.getBytes("key").array();
            boolean added = vertByteBuffers.add(row.getBytes("key"));
            System.out.println(Arrays.hashCode(array));
            System.out.println("added: " + added);
            System.out.println("column1: " + row.getBytes("column1"));
            System.out.println("value: " + row.getBytes("value"));
//            System.out.println(Arrays.toString(row.getBytes(0).array()));
//            if (rowId == 2)
//                break;
        }
        System.out.println("size: " + vertByteBuffers.size());
        cluster.close();
    }
}
