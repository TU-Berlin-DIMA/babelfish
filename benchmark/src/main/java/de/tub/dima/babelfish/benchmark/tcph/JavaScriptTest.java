package de.tub.dima.babelfish.benchmark.tcph;

import de.tub.dima.babelfish.storage.UnsafeUtils;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

public class JavaScriptTest {


    public static void main(String[] a) throws InterruptedException {

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("im tired");
                UnsafeUtils.UNSAFE.park(true, System.currentTimeMillis() + 10000);
                System.out.println("woke up");
            }
        });
        thread.start();

        Thread.sleep(5000);
        System.out.println("waikup other");
        UnsafeUtils.UNSAFE.unpark(thread);


        System.setProperty("luth.home", ".");
        System.setProperty("js.home", ".");
        Context context = Context.newBuilder("js", "python").allowAllAccess(true).
                allowExperimentalOptions(true)
                .option("engine.Mode", "throughput").build();

        Value res = context.eval("js", "" +
                "t = (function () {\n" +
                " var y = 99;\n" +
                " var pol= Polyglot.eval(\"python\", \"lambda z: z.v+42\");" +
                " function mul(x) {\n" +
                "z = {v:x*10};" +
                "    return x*pol(z);\n" +
                " }\n" +
                " return mul;\n" +
                "})()");

        int s = 0;
        for (int i = 0; i < 1000000; i++)
            s = res.execute(10).asInt();
        Thread.sleep(10000);
        for (int i = 0; i < 10000000; i++)
            s = res.execute(10).asInt();
        Thread.sleep(10000);
        System.out.println(s);

    }
}
