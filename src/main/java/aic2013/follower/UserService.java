/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package aic2013.follower;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.PersistenceException;

import aic2013.common.entities.TwitterUser;

/**
 *
 * @author Christian
 */
public class UserService {

    private final EntityManager em;

    public UserService(EntityManager em) {
        this.em = em;
    }

    public boolean persist(TwitterUser user) {
        EntityTransaction tx = null;
        boolean started = false;
        boolean success = false;

        try {
            tx = em.getTransaction();

            if (!tx.isActive()) {
                started = true;
                tx.begin();
            }
            
            em.persist(user);
            em.flush();

            if (started) {
                tx.commit();
                success = true;
            }
        } catch(PersistenceException e){
            success = false;
            
            if (tx != null && tx.isActive()) {
                tx.setRollbackOnly();
            }
        } catch (RuntimeException ex) {
            if (tx != null && tx.isActive()) {
                tx.setRollbackOnly();
            }
            
            throw ex;
        } finally {
            if (tx != null && started && tx.isActive() && tx.getRollbackOnly()) {
                tx.rollback();
            }
        }
        
        return success;
    }

}
