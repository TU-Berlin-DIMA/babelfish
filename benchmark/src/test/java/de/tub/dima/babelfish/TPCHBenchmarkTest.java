package de.tub.dima.babelfish;

import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.benchmark.tcph.RelationalBenchmarkRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.ArrayList;

@RunWith(Parameterized.class)
public class TPCHBenchmarkTest {

    @Parameters(name = "{0}")
    public static Iterable<Object[]>  parameters() {
        ArrayList<Object[]> parameters = new ArrayList<>();
        String[] languages = {"rel", "js", "java", "python"};
        for(String lang: languages){
            parameters.add(new String[]{lang});
        }
        return parameters;
    }

    @Parameterized.Parameter(0)
    public String lang;


    @Test
    public void runQ1() throws InterruptedException, IOException, SchemaExtractionException {
        new RelationalBenchmarkRunner(new String[]{"q1", this.lang, "true"});
    }

    @Test
    public void runQ3() throws InterruptedException, IOException, SchemaExtractionException {
        new RelationalBenchmarkRunner(new String[]{"q3", this.lang, "true"});
    }

    @Test
    public void runQ6() throws InterruptedException, IOException, SchemaExtractionException {
        new RelationalBenchmarkRunner(new String[]{"q6", this.lang, "true"});
    }

    @Test
    public void runQ18() throws InterruptedException, IOException, SchemaExtractionException {
        new RelationalBenchmarkRunner(new String[]{"q18", this.lang, "true"});
    }

    @Test
    public void runSSB11() throws InterruptedException, IOException, SchemaExtractionException {
        new RelationalBenchmarkRunner(new String[]{"ssb11", this.lang, "true"});
    }

    @Test
    public void runSSB21() throws InterruptedException, IOException, SchemaExtractionException {
        new RelationalBenchmarkRunner(new String[]{"ssb21", this.lang, "true"});
    }

    @Test
    public void runSSB31() throws InterruptedException, IOException, SchemaExtractionException {
        new RelationalBenchmarkRunner(new String[]{"ssb31", this.lang, "true"});
    }

    @Test
    public void runSSB41() throws InterruptedException, IOException, SchemaExtractionException {
        new RelationalBenchmarkRunner(new String[]{"ssb41", this.lang, "true"});
    }

}
