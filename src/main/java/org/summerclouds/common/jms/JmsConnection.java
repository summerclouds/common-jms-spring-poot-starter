/**
 * Copyright (C) 2020 Mike Hummel (mh@mhus.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.summerclouds.common.jms;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.summerclouds.common.core.cfg.CfgString;
import org.summerclouds.common.core.tool.MSystem;

public class JmsConnection extends JmsObject implements ExceptionListener {

    private static final Boolean NON_TRANSACTED = false;

    private static final CfgString allowedSerializablePackages =
            new CfgString(
                    JmsConnection.class,
                    "org.apache.activemq.SERIALIZABLE_PACKAGES",
                    "de,java,org,com");

    private Connection connection;
    private ActiveMQConnectionFactory connectionFactory;
    private Session session;

    private String url;

    private String user;

    public JmsConnection(String url, String user, String password) throws JMSException {
        connectionFactory = new ActiveMQConnectionFactory(user, password, url);
        this.url = url;
        this.user = user;
    }

    @Override
    public synchronized void open() throws JMSException {
        System.setProperty("org.apache.activemq.SERIALIZABLE_PACKAGES", allowedSerializablePackages.value());
        if (isClosed()) throw new JMSException("connection closed");
        if (connection == null) {
            log().i("connect", url);
            Connection con = connectionFactory.createConnection();
            con.start();
            connection = con;
            connection.setExceptionListener(this);
            session = connection.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
        }
    }

    @SuppressWarnings("resource")
    public JmsDestination createTopic(String name) throws JMSException {
        return new JmsDestination(name, true).setConnection(this);
    }

    @SuppressWarnings("resource")
    public JmsDestination createQueue(String name) throws JMSException {
        return new JmsDestination(name, false).setConnection(this);
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public void reset() {
        log().i("reset", connection);
        try {
            if (session != null) session.close();
        } catch (Throwable t) {
            log().t(t);
        }
        try {
            if (connection != null) connection.close();
        } catch (Throwable t) {
            log().t(t);
        }
        connection = null;
        session = null;
    }

    @Override
    public void onException(JMSException exception) {
        log().w("kill connection", connection, exception);
        if (exception != null && exception.getMessage() != null) {
            if (exception
                    .getMessage()
                    .contains("Cannot remove a consumer that had not been registered")) return;
        }
        reset();
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    @Override
    public boolean isConnected() {
        return connection != null;
    }

    @Override
    public JmsDestination getJmsDestination() {
        return null;
    }

    @Override
    public String toString() {
        return MSystem.toString(this, url, user);
    }
}
