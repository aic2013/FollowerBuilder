package aic2013.follower;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.neo4j.jdbc.Driver;
import org.neo4j.jdbc.Neo4jConnection;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.PropertyConfiguration;
import twitter4j.json.DataObjectFactory;
import aic2013.common.entities.TwitterUser;
import aic2013.common.service.ClosedException;
import aic2013.common.service.Neo4jService;
import aic2013.common.service.Neo4jUnitOfWork;
import aic2013.common.service.Processor;

public class Consumer implements MessageListener {

    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String FOLLOWS_QUEUE_NAME = "tweet-follows";
    private static final String NEO4J_JDBC_URL = "jdbc:neo4j://localhost:7474";

    private final UserService userService;
    private final Neo4jService neo4jService;
    private final TwitterDataAccess twitterDataAccess;
    private final ConnectionFactory factory;
    private final Connection connection;
    private final Session session;
    private final MessageConsumer consumer;

    private static String getProperty(String name, String defaultValue) {
        String value = System.getProperty(name);

        if (value == null) {
            value = System.getenv(name);
        }
        if (value == null) {
            value = defaultValue;
        }

        return value;
    }

    public static void main(String[] args) throws Exception {
        String brokerUrl = getProperty("BROKER_URL", BROKER_URL);
        String followsQueueName = getProperty("FOLLOWS_QUEUE_NAME", FOLLOWS_QUEUE_NAME);
        String neo4jJdbcUrl = getProperty("NEO4J_JDBC_URL", NEO4J_JDBC_URL);

        Consumer consumer = null;
        EntityManagerFactory emf = null;
        Neo4jConnection neo4j = null;
        Twitter twitter = null;

        try {
            // Database configuration
            Properties prop = new Properties();
            File propertiesFile = new File("hibernate.properties");

            if (propertiesFile.exists()) {
                // Use hibernate properties of the current working directory if exists
                prop.load(new FileInputStream(propertiesFile));
            } else {
                // Fallback to hibernate properties of the classpath
                prop.load(Consumer.class.getClassLoader()
                    .getResourceAsStream("hibernate.properties"));
                propertiesFile = null;
            }

            // RDBMS
            emf = Persistence.createEntityManagerFactory("twitterdb", prop);
            // Neo4j
            neo4j = new Driver().connect(neo4jJdbcUrl, new Properties());
            neo4j.setAutoCommit(true);//false);
            // Twitter
            File twitter4jPropertyFile = new File(getProperty("TWITTER_CONFIG", "twitter4j.properties"));
            
            if(twitter4jPropertyFile.exists()) {
                try(InputStream is = new FileInputStream(twitter4jPropertyFile)) {
                    twitter = new TwitterFactory(new PropertyConfiguration(is)).getInstance();
                } catch(IOException ex) {
                    throw new RuntimeException("Could not read twitter4j.properties file", ex);
                }
            } else {
                twitter = TwitterFactory.getSingleton();
            }
            

            // Services
            UserService userService = new UserService(emf.createEntityManager());
            Neo4jService neo4jService = new Neo4jService(neo4j);
            TwitterDataAccess twitterDataAccess = new TwitterDataAccess(twitter);

            ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
            consumer = new Consumer(userService, neo4jService, twitterDataAccess, factory, followsQueueName);
            consumer.consumer.setMessageListener(consumer);

            System.out.println("Started follwer consumer with the following configuration:");
            System.out.println("\tBroker: " + brokerUrl);
            System.out.println("\t\tFollows queue name: " + followsQueueName);
            System.out.println("\tJPA-Unit: twitterdb");
            System.out.println("\t\thibernate.properties: " + (propertiesFile == null ? "from classpath" : propertiesFile
                .getAbsolutePath()));
            System.out.println("\tNeo4j: " + neo4jJdbcUrl);
            System.out.println();
            System.out.println("To shutdown the application please type 'exit'.");

            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String command;

            while ((command = br.readLine()) != null) {
                if ("exit".equals(command)) {
                    break;
                }
            }

        } finally {
            if (consumer != null) {
                consumer.close();
            }
            if (emf != null && emf.isOpen()) {
                emf.close();
            }
            if (neo4j != null) {
                neo4j.close();
            }
            if (twitter != null) {
                twitter.shutdown();
            }
        }
    }

    public Consumer(UserService userService, Neo4jService neo4jService, TwitterDataAccess twitterDataAccess, ConnectionFactory factory, String queueName)
        throws JMSException {
        this.userService = userService;
        this.neo4jService = neo4jService;
        this.twitterDataAccess = twitterDataAccess;
        this.factory = factory;
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = session.createConsumer(session.createQueue(queueName));
    }

    public void close() {
        if(twitterDataAccess != null) {
            twitterDataAccess.close();
        }
        if(neo4jService != null) {
            neo4jService.close();
        }
        if (consumer != null) {
            try {
                consumer.close();
            } catch (JMSException ex) {
                Logger.getLogger(Consumer.class.getName())
                    .log(Level.SEVERE, null, ex);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException ex) {
                Logger.getLogger(Consumer.class.getName())
                    .log(Level.SEVERE, null, ex);
            }
        }
    }

    @Override
    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage) {
                final Status status = DataObjectFactory.createStatus(((TextMessage) message).getText());
                final TwitterUser user = new TwitterUser(status.getUser());

                if (userService.persist(user)) {
                    // If the user was actually added, do the neo4j stuff
                    neo4jService.transactional(new Neo4jUnitOfWork() {

                        @Override
                        public void process() {
                            neo4jService.createPersonIfAbsent(user);
                            twitterDataAccess.forAllFriendIds(user, new Processor<Long>() {

                                @Override
                                public void process(Long userId) {
                                    TwitterUser friend = new TwitterUser(userId);
                                    neo4jService.createPersonIfAbsent(friend);
                                    neo4jService.createUniqueRelation(user, "FOLLOWS", friend);
                                }
                            });
                        }
                    });
                }
            }
            
            message.acknowledge();
        } catch (JMSException ex) {
            Logger.getLogger(Consumer.class.getName())
                .log(Level.SEVERE, null, ex);
        } catch (TwitterException ex) {
            Logger.getLogger(Consumer.class.getName())
                .log(Level.SEVERE, null, ex);
        } catch (ClosedException ex) {
            // Clean close
        } catch (RuntimeException ex) {
            Logger.getLogger(Consumer.class.getName())
                .log(Level.SEVERE, null, ex);
        }
    }
}
