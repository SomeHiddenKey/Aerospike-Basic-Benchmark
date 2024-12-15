package org.aerobenchmark;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Test;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public abstract class BenchmarkBase {
    private final static Integer MEASUREMENT_ITERATIONS = 3;
    private final static Integer WARMUP_ITERATIONS = 3;

    @Test
    public void executeJmhRunner() throws RunnerException {
        Options opt = new OptionsBuilder()
                // set the class name regex for benchmarks to search for to the current class
                .include("\\." + this.getClass().getSimpleName() + "\\.")
                .warmupIterations(WARMUP_ITERATIONS)
                .measurementIterations(MEASUREMENT_ITERATIONS)
                // do not use forking or the benchmark methods will not see references stored within its class
                .forks(0)
                // do not use multiple threads
                .threads(1)
                .shouldDoGC(true)
                .shouldFailOnError(true)
                .resultFormat(ResultFormatType.JSON)
                .result("Benchmark_Data_1.json") // set this to a valid filename if you want reports
                .shouldFailOnError(true)
                .jvmArgs("-server")
                .build();

        new Runner(opt).run();
    }

    //Creates a secundary index for a given label key, if needed
    public static void create_sec_index(GraphTraversalSource g, String key){
        g.call("aerospike.graph.admin.index.create").
                with("element_type", "vertex").
                with("property_key", key).next();
    }
}