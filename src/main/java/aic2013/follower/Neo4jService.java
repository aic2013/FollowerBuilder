/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package aic2013.follower;

import aic2013.follower.entities.TwitterUser;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.jdbc.Neo4jConnection;
import org.restlet.resource.ResourceException;

/**
 *
 * @author Christian
 */
public class Neo4jService {

    private static final int MAX_RETRIES = 3;
    private final Neo4jConnection connection;
    private volatile boolean open = true;

    public Neo4jService(Neo4jConnection connection) {
        this.connection = connection;
    }
    
    public void close() {
        open = false;
    }

    public void transactional(Neo4jUnitOfWork r) {
        boolean rollbackOnly = false;

        try {
            r.process();
        } catch (RuntimeException ex) {
            rollbackOnly = true;
            throw ex;
        } finally {
//            try {
//                if (rollbackOnly) {
//                    connection.rollback();
//                } else {
//                    connection.commit();
//                }
//            } catch (SQLException ex) {
//                Logger.getLogger(Neo4jService.class.getName())
//                    .log(Level.SEVERE, null, ex);
//            }
        }
    }

    public void createPersonIfAbsent(TwitterUser user) {
        StringBuilder sb = new StringBuilder().append("MERGE (pers:")
            .append(user.toNeo4j())
            .append(")");
        query(sb.toString());
    }

    public void createUniqueRelation(TwitterUser user, String relation, TwitterUser friend) {
        StringBuilder sb = new StringBuilder().append("MATCH (pers:")
            .append(user.toNeo4j())
            .append("),")
            .append("(friend:")
            .append(friend.toNeo4j())
            .append(")\n")
            .append("CREATE UNIQUE (pers)-[:")
            .append(relation)
            .append("]->(friend)");
        query(sb.toString());
    }

    private void query(String query) {
        Statement statement = null;
        boolean done = false;

        while(!done && open) {
            if(!open) {
                throw new ClosedException();
            }
            
            try {
                statement = connection.createStatement();
                statement.executeUpdate(query);
                done = true;
            } catch (SQLException ex) {
                if(ex.getCause() instanceof ResourceException) {
                    if(((ResourceException) ex.getCause()).getStatus().getCode() > 1000) {
                        // If we see a connection related error try to sleep for a bit
                        sleepOrClose(1000);
                    }
                }
                Logger.getLogger(Neo4jService.class.getName())
                    .log(Level.SEVERE, "Retrying to execute query", ex);
            } finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException ex) {
                        // Ignore
                    }
                }
            }
        }
    }
    
    private void sleepOrClose(int sleepTime) {
        int sleptTime = 0;

        while(sleptTime < sleepTime) {
            try {
                Thread.sleep(sleepTime - sleptTime);
            } catch (InterruptedException ex) {
                if(!open) {
                    throw new ClosedException();
                }
            }
        }
    }
}
