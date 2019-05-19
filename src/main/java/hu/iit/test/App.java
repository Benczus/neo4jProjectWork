package hu.iit.test;


import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.graphdb.Label;

import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 *
 */


public class App
{
    public static void main( String[] args ) {
        MovieDatabase databaseOperations = new MovieDatabase(GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "cica")));

        StatementResult result = databaseOperations.queryCoactorsWithActor("\"Kevin Bacon\"");
        printer(result);



    }

    private static void printer(StatementResult result) {
        while (result.hasNext()) {
            Record coActors = result.next();
            System.out.println(coActors.values());

        }

    }
}


