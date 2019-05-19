package hu.iit.test;

import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Str;
import org.neo4j.driver.v1.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class MovieDatabase {

    public MovieDatabase(Driver driver) {
        this.driver = driver;
        session= driver.session();
    }

    private  Driver driver ;//= GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "cica"));
    private  Session session;

    //    actor út
    public  StatementResult shortestPathBetweenActors(String actorName1, String actorName2) {
        try (Transaction transaction = session.beginTransaction()) {
            StatementResult result = transaction.run("MATCH p=shortestPath(" +
                    "(actor1:Person {name:" + actorName1 + "})-[*]-(actor2:Person {name:" + actorName2 + "})) " +
                    "RETURN p");
            transaction.success();
            return result;
        }
    }
//    Kik játszottak együtt Tom Hanks-el?
//
//
    public  StatementResult queryCoactorsWithActor(String actorName) {
        StatementResult result;
//
                try (Transaction transaction = session.beginTransaction()) {
             result = transaction.run("MATCH (actor:Person {name:" + actorName + "})" +
                    "-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(coActors) RETURN coActors.name ");
            transaction.success();

        }
        return result;
    }
//
//
    //    Valaki mutassa be Tom Hanks-et, Keanu Reeves-nek.
    public  StatementResult queryRelationshipBetweenTwoActors(String actorName, String actorName2) {
        try (Transaction transaction = session.beginTransaction()) {
            StatementResult result = transaction.run("MATCH (tom:Person {name:" + actorName + "})-[:ACTED_IN]->(m)" +
                    "<-[:ACTED_IN]-(coActors),(coActors)-[:ACTED_IN]->(m2)<-[:ACTED_IN]-(neo:Person {name:" + actorName2 + "})\n" +
                    "RETURN tom, m, coActors, m2, neo");
            transaction.success();
            return result;
        }
    }

    //    Keanu Reeves és Val Kilmer között egy út, amin rajta van Kevin actor.
    public  StatementResult queryRelationshipBetweenTwoActorsWithActorInRelationship(String actorName1, String actorName2, String actorName3) {
        try (Transaction transaction = session.beginTransaction()) {
            StatementResult result = transaction.run("MATCH p=shortestPath(" +
                    "(neo:Person {name:" + actorName1 + "})-[*]-(actor:Person {name:" + actorName3 + "}))," +
                    " s=shortestPath((actor)-[*]-(val:Person {name:" + actorName2 + "})) RETURN p,s");
            transaction.success();
            return result;
        }
    }

    //    Akik rendezték a Cloud Atlas-t, milyen más filmet rendeztek még?
    public  StatementResult queryOtherMoviesByMovie(String movieName) {
        try (Transaction transaction = session.beginTransaction()) {
            StatementResult result = transaction.run("MATCH (cloudAtlas {title: " + movieName + "})" +
                    "<-[:DIRECTED]-(directors:Person)-[:DIRECTED]->(movies) RETURN directors, movies");
            transaction.success();
            return result;
        }
    }

    //    Milyen kapcsolatban állnak emberek a Mátrix-al?
    public  StatementResult queryRelationshipWithMovie(String movieName) {
        try (Transaction transaction = session.beginTransaction()) {
            StatementResult result = transaction.run("MATCH (people:Person)-[relatedTo]-(m:Movie {title: " + movieName + "})" +
                    " RETURN people.name, Type(relatedTo), relatedTo");
            transaction.success();
            return result;
        }
    }

    //    Olyan filmek, amik 1990-2000 között készültek, Tom Hanks szerepel bennük, plusz ezen filmek többi szereplõi.
    public  StatementResult queryFilmsBetweenYearsWithActor(int year1, int year2, String actor) {
        try (Transaction transaction = session.beginTransaction()) {
            StatementResult result = transaction.run("MATCH (coStars:Person)-[:ACTED_IN]->(nineties:Movie)" +
                    "<-[:ACTED_IN]-(actor:Person {name:" + actor + "}) WHERE nineties.released >= " + year1 + " AND " +
                    "nineties.released < " + year2 + " RETURN nineties.title, coStars.name");
            transaction.success();
            return result;
        }
    }
//    Adjuk meg azokat a színészeket, akiket ajánlottak Kevin actor-nek, hogy dolgozzanak együtt, úgy hogy:
//            - még nem dolgoztak együtt,
//	          - csökkenõ sorrendben, hogy hány színész társ-on keresztül ismerheti meg az illetõt,
//	          (hányszor ajánlhatták neki különbözõ színész társak)
//            - az elsõ 5 ilyen színészt csak.

    public  StatementResult queryRecommendedCoActorsToActors(String actor) {
        try (Transaction transaction = session.beginTransaction()) {
            StatementResult result = transaction.run("MATCH (actor:Person {name:" + actor + "})" +
                    "-[:ACTED_IN]->(m)<-[:ACTED_IN]-(coActors), " +
                    "(coActors)-[:ACTED_IN]->(m2)<-[:ACTED_IN]-(cocoActors) WHERE NOT " +
                    "(actor)-[:ACTED_IN]->()<-[:ACTED_IN]-(cocoActors) AND actor <> cocoActors " +
                    "RETURN cocoActors.name AS Recommended, count(*) AS Count ORDER BY Count DESC LIMIT 5");
            transaction.success();
            return result;
        }

    }

    public  StatementResult createNewNode(String nodeName, String nodeType, Map<String, String> nodeMap) {
        StatementResult result;
        String labels="";
        int i=0;
        try (Transaction transaction = session.beginTransaction()) {
            labels= createLabels(nodeMap);
            result = transaction.run("CREATE ("+nodeName+":"+nodeType+" {"+labels+"})");
        }
        return result;
    }

    public  StatementResult createNewConnection(String nodeName1, String nodeName2, String connectionLabel) {
        StatementResult result;
        try (Transaction transaction = session.beginTransaction()) {
            result = transaction.run("("+nodeName1+")-[:"+connectionLabel+"]->("+nodeName2+")");
        }
        return result;

    }

    public  StatementResult createNodesWithConnection(String nodeName, String nodeType, Map<String, String> nodeMap, Map<String, String> connections) {
        StatementResult result;
        try (Transaction transaction = session.beginTransaction()) {
            result = createNewNode(nodeName, nodeType, nodeMap);
            for (Map.Entry entry : connections.entrySet()){
              result= createNewConnection(nodeName, entry.getValue().toString(), entry.getKey().toString());
            }
        }
        return result;
    }

    public  StatementResult updateNode(String matchProperty, String propertyValue, Map<String, String> labelsMap) {
        StatementResult result;
        String labels="";
        int i=0;
        try (Transaction transaction = session.beginTransaction()) {
            labels= createLabels(labelsMap);
            result=transaction.run("MATCH (p { +"+matchProperty+"+: '"+propertyValue+"' }) SET p = { "+labels+" }");
        }
        return result;
    }



    private String createLabels(Map<String, String> labelsMap) {
        StringBuilder labels = new StringBuilder(); int i = 0;
        for (Map.Entry entry : labelsMap.entrySet()){
            labels.append((i + 1 != labelsMap.size()) ? entry.getKey() + ":" + entry.getValue() + "," : entry.getKey() + ":" + entry.getValue());
            i++;
        }
        return labels.toString();
    }



}
