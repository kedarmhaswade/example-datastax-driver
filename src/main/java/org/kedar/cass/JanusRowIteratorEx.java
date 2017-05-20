package org.kedar.cass;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by kedar on 5/17/17.
 */
public class JanusRowIteratorEx {
    public static void main(String[] args) throws Exception {
        String serverIP = "127.0.0.1";
        String keyspace = "janusgraph";
        String table = "edgestore";


        try (Cluster cluster = Cluster.builder()
            .addContactPoints(serverIP)
            .build()) {

            Session session = cluster.connect(keyspace);
            String cqlStatement = "SELECT * FROM " + table;
            int nRows = 0;
            Set<ByteBuffer> keys = new HashSet<>();
            for (Row row : session.execute(cqlStatement)) {
                if (nRows == 0)
                    printRowMetadata(row);
                nRows++;
                keys.add(row.get(0, ByteBuffer.class));
            }
            System.out.println("number of rows in the table: " + nRows);
            System.out.println("number of distinct keys in the table: " + keys.size());
        }
    }

    private static void printRowMetadata(Row row) {
        ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        System.out.println("number of columns: " + columnDefinitions.size());
        for (ColumnDefinitions.Definition d : columnDefinitions) {
            System.out.println("column name: " + d.getName() + ", type: " + d.getType());
        }
        System.out.println("is key the name of 0th column of row? " + row.getObject(0).equals(row.getObject("key")));
        System.out.println("is column1 the name of 1st column of row? " + row.getObject(1).equals(row.getObject("column1")));
        System.out.println("is value the name of 2nd column of row? " + row.getObject(2).equals(row.getObject("value")));
    }
}
