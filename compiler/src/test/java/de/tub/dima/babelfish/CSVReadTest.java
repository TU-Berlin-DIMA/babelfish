package de.tub.dima.babelfish;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CSVReadTest {

    @Test
    public void readCSVTest() throws IOException {


        byte[] array = Files.readAllBytes(Paths.get("/tpch/tpch-dbgen/orders.tbl"));

        int field = 15;
        for (int i = 0; i < array.length; i++) {


            for(int f = 0; f < 15; f++) {
                // skip field
                while (array[i] != '|') {
                    i++;
                }
                System.out.println(i);
                i++;
            }
            if (array[i] == '\n') {
                System.out.println("record edge");
            }
        }

    }


}
