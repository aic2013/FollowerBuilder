/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package aic2013.follower;

import aic2013.follower.entities.TwitterUser;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import twitter4j.IDs;
import twitter4j.PagableResponseList;
import twitter4j.RateLimitStatus;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.User;

/**
 *
 * @author Christian
 */
public class TwitterDataAccess {

    private final Twitter twitter;
    private volatile boolean open = true;

    public TwitterDataAccess(Twitter twitter) {
        this.twitter = twitter;
    }
    
    public void close() {
        open = false;
    }
    
    public void forAllFriendIds(TwitterUser twitterUser, Processor<Long> processor) {
        long nextCursor = -1;
        IDs usersResponse = null;
        
        do {
            assert usersResponse == null;
            
            while(usersResponse == null) {
                if(!open) {
                    throw new ClosedException();
                }
                    
                try {
                     usersResponse = twitter.getFriendsIDs(twitterUser.getId(), nextCursor);
                } catch (TwitterException ex) {
                    if(ex.exceededRateLimitation()) {
                        waitForReset(ex.getRateLimitStatus().getSecondsUntilReset() * 1000);
                    } else if(ex.resourceNotFound()) {
//                        throw new RuntimeException("The user for which the friends were requested does not exist any more.", ex);
                        return;
                    }
                    
                    // Retry when other errors like IO-related ones occur
                }
            }
            
            assert usersResponse != null;
            nextCursor = usersResponse.getNextCursor();

            for (long userId : usersResponse.getIDs()) {
                processor.process(userId);
            }
            usersResponse = null;
        } while (nextCursor > 0);
    }
    
    private void waitForReset(int sleepTime) {
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
