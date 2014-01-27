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

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.neo4j.jdbc.Driver;
import org.neo4j.jdbc.Neo4jConnection;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.json.DataObjectFactory;
import aic2013.common.entities.TwitterUser;
import aic2013.common.service.ClosedException;
import aic2013.common.service.Neo4jService;
import aic2013.common.service.Neo4jUnitOfWork;
import aic2013.common.service.Processor;

public class FollowerBuilder {

    private static final String BROKER_URL = "amqp://localhost:5672/test";
    private static final String FOLLOWS_QUEUE_NAME = "tweet-follows";
    private static final String NEO4J_JDBC_URL = "jdbc:neo4j://localhost:7474";

    private final UserService userService;
    private final Neo4jService neo4jService;
    private final TwitterDataAccess twitterDataAccess;
    private final ConnectionFactory factory;
    private final Connection connection;
    private final Channel channel;
    private final Consumer consumer;

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

        FollowerBuilder consumer = null;
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
                prop.load(FollowerBuilder.class.getClassLoader()
                    .getResourceAsStream("hibernate.properties"));
                propertiesFile = null;
            }

            // RDBMS
            emf = Persistence.createEntityManagerFactory("twitterdb", prop);
            // Neo4j
            neo4j = new Driver().connect(neo4jJdbcUrl, new Properties());
            neo4j.setAutoCommit(true);//false);
            // Twitter
            twitter = TwitterFactory.getSingleton();

            // Services
            UserService userService = new UserService(emf.createEntityManager());
            Neo4jService neo4jService = new Neo4jService(neo4j);
            TwitterDataAccess twitterDataAccess = new TwitterDataAccess(twitter);

            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername("test");
            factory.setPassword("test");
            factory.setUri(brokerUrl);
            consumer = new FollowerBuilder(userService, neo4jService, twitterDataAccess, factory, followsQueueName);

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

    public FollowerBuilder(UserService userService, Neo4jService neo4jService, TwitterDataAccess twitterDataAccess, ConnectionFactory factory, String queueName)
        throws IOException {
        this.userService = userService;
        this.neo4jService = neo4jService;
        this.twitterDataAccess = twitterDataAccess;
        this.factory = factory;
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(queueName, true, false, false, null);
        channel.basicQos(1);

        consumer = createMessageConsumer(channel);
        channel.basicConsume(queueName, false, consumer);
    }

    public void close() {
        if(twitterDataAccess != null) {
            twitterDataAccess.close();
        }
        if(neo4jService != null) {
            neo4jService.close();
        }
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ex) {
                Logger.getLogger(FollowerBuilder.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException ex) {
                Logger.getLogger(FollowerBuilder.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private DefaultConsumer createMessageConsumer(Channel aChannel) {
        return new DefaultConsumer(aChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    String message = new String(body);

                    final Status status = DataObjectFactory.createStatus(message);
                    final TwitterUser user = new TwitterUser(status.getUser());

                    if (userService.persist(user)) {
                        // If the user was actually added, do the neo4j stuff
                        neo4jService.transactional(new Neo4jUnitOfWork() {
                            @Override
                            public void process() {
                                neo4jService.createPersonIfAbsent(user);
                            }
                        });
                    }

                    channel.basicAck(envelope.getDeliveryTag(), false);
                } catch (TwitterException ex) {
                    Logger.getLogger(FollowerBuilder.class.getName()).log(Level.SEVERE, null, ex);
                } catch (ClosedException ex) {
                    // Clean close
                } catch (RuntimeException ex) {
                    Logger.getLogger(FollowerBuilder.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };
    }
}
