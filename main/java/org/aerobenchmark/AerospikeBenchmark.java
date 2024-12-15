package org.aerobenchmark;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class AerospikeBenchmark extends BenchmarkBase{
    private static String host = "127.0.0.1";
    private static int port = 3000;
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(host).port(port).enableSsl(false);
    final Cluster cluster = BUILDER.create();
    final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
    String random_actor_id = "";
    String random_director_id = "";
    String random_writer_id = "";
    String random_movie_id = "";
    Pair<Integer, Integer> random_time_interval = new Pair<Integer, Integer>(1980, 2001);

    @Benchmark
    public void query1(){
        g.V(random_actor_id)
            .out("knownForTitle")
            .values("primaryTitle").toList();
    }

    //Get first and last movie a given actor is known for
    @Benchmark
    public List<Object> query2(){
        return g.V(random_actor_id)
            .out("knownForTitle")
            .union(
                limit(1),
                tail()
            ).values("name").toList();
    }

    //Get the director who directed the most movies
    @Benchmark
    public String query3(){
        return g.V()
            .filter(enforce_profession("director"))
            .order().by(outE("directed").count())
            .tail().values("name").next().toString();
    }

    //Get the director who directed the most movies during a specific period of time
    @Benchmark
    public String query4(){
        return g.V()
            .filter(enforce_profession("director"))
            .order().by(out("directed").has("startYear",gte(random_time_interval.getValue0())).has("endYear",lte(random_time_interval.getValue1())).count())
            .tail().values("name").next().toString();
    }

    //Get the actor with the most diversified genres of movie
    @Benchmark
    public String query5(){
        return g.V()
            .filter(enforce_profession("actor"))
            .order().by(out("knownFor").out("genre").dedup().count())
            .tail().values("name").next().toString();
    }

    //Get all the actors who worked with a given writer
    @Benchmark
    public List<Object> query6(){
        return g.V(random_writer_id)
            .out("wrote")
            .in("knownFor")
            .filter(enforce_profession("actor"))
            .dedup()
            .values("name").toList();
    }

    //Get all the actors who never worked with a given writer
    @Benchmark
    public List<Object> query7(){
        return g.V()
            .filter(enforce_profession("actor"))
            .not(out("knownFor").in("wrote").hasId(random_writer_id))
            .values("name").toList();
    }

    //Get all the writers that wrote at least 2 movies
    @Benchmark
    public List<Object> query8(){
        return g.V()
            .where(out("wrote").count().is(gte(2)))
            .values("name").toList();
    }

    //Get all the people who worked on a movie
    @Benchmark
    public List<Object> query9(){
        return g.V(random_movie_id)
            .union(
                in("knownForTitle"),
                in("directed"),
                in("wrote")
            )
            .dedup()
            .values("name").toList();
    }

    //Get the movie that employed the most people
    @Benchmark
    public String query10(){
        return g.V()
            .order().by(
                union(
                    in("knownForTitle"),
                    in("directed"),
                    in("wrote")
                )
                .dedup()
                .count()
            )
            .tail().values("primaryTitle").next().toString();
    }

    //Sort movies alphabetically
    public void query11(){
        g.V()
            .order().by("primaryTitle")
            .values("primaryTitle");
    }

    public static Predicate<Traverser<Vertex>> enforce_profession(String profession){
        return (traverser -> {
            List<String> company = traverser.get().value("professions");
            return company.contains(profession);
        });
    }
}


