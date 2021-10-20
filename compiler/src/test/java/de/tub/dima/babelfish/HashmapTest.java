package de.tub.dima.babelfish;

import de.tub.dima.babelfish.ir.pqp.objects.state.map.HashMap;
import org.junit.Test;

public class HashmapTest {

    public void inserts(HashMap map2){
        HashMap map = new HashMap(110, 8);
        for (int i = 0; i < 100000; i++) {
            map.put(i%100, i);
        }
    }

    @Test
    public void HashmapTest() {
        HashMap map = new HashMap(110, 8);
        map.put(1, 1);
        map.put(1, 1);
        map.put(2, 1);
        map.put(2, 1);
        map.put(5, 1);
        map.put(6, 1);
        map.put(7, 1);
        map.put(8, 1);
        map.put(9, 1);
        System.out.println(map.toString());

        for(int i = 0; i< 10000;i++){
            long startTime = System.currentTimeMillis();
            inserts(map);
            System.out.println(System.currentTimeMillis() - startTime);
        }
        System.out.println(map.toString());
    }


}
