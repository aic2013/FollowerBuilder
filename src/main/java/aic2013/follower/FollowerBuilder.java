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

    private final UserService userService;
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
        final EntityManagerFactory emf = Persistence.createEntityManagerFactory("twitterdb", prop);
        // Twitter
        final Twitter twitter = TwitterFactory.getSingleton();

        // Services
        UserService userService = new UserService(emf.createEntityManager());
        TwitterDataAccess twitterDataAccess = new TwitterDataAccess(twitter);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("test");
        factory.setPassword("test");
        factory.setUri(brokerUrl);

        final FollowerBuilder consumer = new FollowerBuilder(userService, twitterDataAccess, factory, followsQueueName);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (consumer != null) {
                    consumer.close();
                }
                if (emf != null && emf.isOpen()) {
                    emf.close();
                }
                if (twitter != null) {
                    twitter.shutdown();
                }

                System.out.println("Exiting");
                System.exit(0);
            }
        });

        System.out.println("Started follwer consumer with the following configuration:");
        System.out.println("\tBroker: " + brokerUrl);
        System.out.println("\t\tFollows queue name: " + followsQueueName);
        System.out.println("\tJPA-Unit: twitterdb");
        System.out.println("\t\thibernate.properties: " + (propertiesFile == null ? "from classpath" : propertiesFile.getAbsolutePath()));
        System.out.println();
        System.out.println("To shutdown the application please type 'exit'.");

        while (true) {
            Thread.sleep(1000);
        }
    }

    public FollowerBuilder(UserService userService, TwitterDataAccess twitterDataAccess, ConnectionFactory factory, String queueName)
        throws IOException {
        this.userService = userService;
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

                    userService.persist(user);

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
