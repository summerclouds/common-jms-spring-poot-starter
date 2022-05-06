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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.TemporaryQueue;

import org.summerclouds.common.core.M;
import org.summerclouds.common.core.cfg.CfgLong;
import org.summerclouds.common.core.security.ISubject;
import org.summerclouds.common.core.tool.MPeriod;
import org.summerclouds.common.core.tool.MSecurity;
import org.summerclouds.common.core.tool.MTracing;
import org.summerclouds.common.core.tracing.IScope;

public class ClientJms extends JmsChannel implements MessageListener {

    private static final CfgLong CFG_ANSWER_TIMEOUT =
            new CfgLong(MJms.class, "answerTimeout", MPeriod.MINUTE_IN_MILLISECONDS * 5);
    private static final CfgLong CFG_WARN_TIMEOUT =
            new CfgLong(MJms.class, "warnTimeout", MPeriod.MINUTE_IN_MILLISECONDS);
    private static final CfgLong CFG_BROADCAST_TIMEOUT =
            new CfgLong(MJms.class, "broadcastTimeout", 5000l);

    private MessageProducer producer;

    private TemporaryQueue answerQueue;
    private MessageConsumer responseConsumer;

    private HashMap<String, Message> responses = null;
    private HashSet<String> allowedIds = new HashSet<>();
    private long timeout = CFG_ANSWER_TIMEOUT.value();
    private long warnTimeout = CFG_WARN_TIMEOUT.value();
    private long broadcastTimeout = CFG_BROADCAST_TIMEOUT.value();

    private JmsInterceptor interceptorOut;
    private JmsInterceptor interceptorIn;

    public ClientJms(JmsDestination dest) {
        super(dest);
    }

    public void sendJmsOneWay(Message msg) throws JMSException {
        try (IScope scope =
                MTracing.enter("jmscall-oneway", "name", getName(), "dest", dest.toString())) {
            open();
            JmsContext context = new JmsContext(msg, MSecurity.getCurrent());
            prepareMessage(msg);
            if (interceptorOut != null) interceptorOut.prepare(context);
            log().d("sendJmsOneWay", dest, producer.getTimeToLive(), msg);
            try {
                producer.send(msg);
            } catch (IllegalStateException ise) {
                log().d("reconnect", getName(), ise.getMessage());
                producer = null;
                open();
                producer.send(msg);
            }
        }
    }

    protected void prepareMessage(Message msg) throws JMSException {

        msg.setJMSMessageID(createMessageId());

        MTracing.get().inject((key, value) -> {
			try {
				msg.setStringProperty(key, value);
			} catch (Throwable t) {
	            log().d(t);
			}
		} );

        ISubject subject = MSecurity.getCurrent();
        if (subject != null) {
            String tokenStr =
                    M.l(TrustApi.class).createToken("jms:" + getJmsDestination(), msg, subject);
            if (tokenStr != null) msg.setStringProperty(M.PARAM_AUTH_TOKEN, tokenStr);
            msg.setStringProperty("_account", String.valueOf(subject.getPrincipal()));
        }
    }

    public Message sendJms(Message msg) throws JMSException {
        try (IScope scope = MTracing.enter(
                                "jmscall " + getName() + "/" + msg.getStringProperty("path"),
                                "name",
                                getName(),
                                "dest",
                                dest.toString())) {
            open();

            JmsContext context = new JmsContext(msg, MSecurity.getCurrent());
            prepareMessage(msg);
            String id = msg.getJMSMessageID();
            openAnswerQueue();
            msg.setJMSReplyTo(answerQueue);
            msg.setJMSCorrelationID(id);
            addAllowedId(id);
            if (interceptorOut != null) interceptorOut.prepare(context);
            try {
                log().d("sendJms", dest, producer.getTimeToLive(), msg);
                try {
                    producer.send(msg);
                } catch (IllegalStateException ise) {
                    log().d("reconnect", getName(), ise.getMessage());
                    producer = null;
                    open();
                    openAnswerQueue();
                    msg.setJMSReplyTo(answerQueue);
                    producer.send(msg);
                }

                long start = System.currentTimeMillis();
                while (true) {
                    try {
                        synchronized (this) {
                            this.wait(10000);
                        }
                    } catch (InterruptedException e) {
                        log().t(e);
                    }

                    synchronized (responses) {
                        Message answer = responses.get(id);
                        if (answer != null) {
                            context.setAnswer(answer);
                            responses.remove(id);
                            log().d("sendJmsAnswer", dest, answer);
                            try {
                                if (interceptorIn != null) interceptorIn.answer(context);
                            } catch (Throwable t) {
                                log().d(t);
                            }
                            return answer;
                        }
                    }

                    long delta = System.currentTimeMillis() - start;
                    if (delta > warnTimeout)
                        log().w("long time waiting", dest, delta, MTracing.getSpanId());

                    if (delta > timeout) {
                        log().w("timeout", delta, MTracing.getSpanId());
                        throw new JMSException("answer timeout " + dest);
                    }
                }
            } finally {
                removeAllowedId(id);
            }
        }
    }

    protected void addAllowedId(String id) {
        synchronized (responses) {
            responses.remove(id);
            allowedIds.add(id);
        }
    }

    protected void removeAllowedId(String id) {
        synchronized (responses) {
            responses.remove(id);
            allowedIds.remove(id);
        }
    }

    public Message[] sendJmsBroadcast(Message msg) throws JMSException {
        open();

        String id = createMessageId();
        msg.setJMSMessageID(id);
        openAnswerQueue();
        msg.setJMSReplyTo(answerQueue);
        msg.setJMSCorrelationID(id);
        addAllowedId(id);
        try {
            log().d("sendJmsBroadcast", dest, producer.getTimeToLive(), msg);
            try {
                producer.send(msg, deliveryMode, getPriority(), getTimeToLive());
            } catch (IllegalStateException ise) {
                log().d("reconnect", getName(), ise.getMessage());
                producer = null;
                open();
                openAnswerQueue();
                msg.setJMSReplyTo(answerQueue);
                producer.send(msg, deliveryMode, getPriority(), getTimeToLive());
            }

            long start = System.currentTimeMillis();
            LinkedList<Message> res = new LinkedList<>();
            while (true) {
                try {
                    synchronized (this) {
                        this.wait(1000);
                    }
                } catch (InterruptedException e) {
                    log().d(e);
                }

                synchronized (responses) {
                    Message answer = responses.get(id);
                    if (answer != null) {
                        responses.remove(id);
                        res.add(answer);
                    }
                }

                long delta = System.currentTimeMillis() - start;
                if (delta > broadcastTimeout) break;
            }

            log().d("sendJmsBroadcastAnswer", dest);
            log().t("sendJmsBroadcastAnswer", dest, res);
            return res.toArray(new Message[res.size()]);
        } catch (JMSException e) {
            reopen();
            throw e;
        } finally {
            removeAllowedId(id);
        }
    }

    @Override
    public synchronized void open() throws JMSException {
        if (isClosed()) throw new JMSException("client closed: " + getName());
        if (producer == null || getSession() == null) {
            dest.open();
            log().d("open", dest);
            producer = dest.getConnection().getSession().createProducer(dest.getDestination());
            if (timeout >= 0) producer.setTimeToLive(timeout);
            // reset answer queue
            try {
                if (answerQueue != null) answerQueue.delete();
            } catch (Throwable t) {
                log().t(t);
            }
            answerQueue = null;
        }
    }

    protected synchronized void openAnswerQueue() throws JMSException {
        if (isClosed()) throw new JMSException("client closed: " + getName());
        if (answerQueue == null || getSession() == null) {
            open();
            answerQueue = dest.getConnection().getSession().createTemporaryQueue();
            responseConsumer = dest.getConnection().getSession().createConsumer(answerQueue);
            responses = new HashMap<>();
            responseConsumer.setMessageListener(this);
        }
    }

    @Override
    public void onMessage(Message message) {
        if (message == null) return;
        try {
            synchronized (responses) {
                String id = message.getJMSCorrelationID();
                if (!allowedIds.contains(id)) return;
                responses.put(id, message);
            }
            synchronized (this) {
                this.notifyAll();
            }
        } catch (JMSException e) {
            log().d(e);
        }
    }

    @Override
    public void reset() {
        log().d("reset", dest);
        try {
            if (producer != null) producer.close();
        } catch (Throwable t) {
            log().d(t);
        }
        try {
            if (responseConsumer != null) responseConsumer.close();
        } catch (Throwable t) {
            log().d(t);
        }
        try {
            if (answerQueue != null) answerQueue.delete();
        } catch (Throwable t) {
            log().d(t);
        }
        producer = null;
        responseConsumer = null;
        answerQueue = null;
    }

    @Override
    public void doBeat() {
        // nothing to do
    }

    @Override
    public String getName() {
        return "openwire:/client" + dest.getName();
    }

    public long getBroadcastTimeout() {
        return broadcastTimeout;
    }

    public void setBroadcastTimeout(long broadcastTimeout) {
        this.broadcastTimeout = broadcastTimeout;
    }

    @Override
    public boolean isConnected() {
        return !(producer == null || getSession() == null);
    }

    public JmsInterceptor getInterceptorOut() {
        return interceptorOut;
    }

    public void setInterceptorOut(JmsInterceptor interceptorOut) {
        this.interceptorOut = interceptorOut;
    }

    public JmsInterceptor getInterceptorIn() {
        return interceptorIn;
    }

    public void setInterceptorIn(JmsInterceptor interceptorIn) {
        this.interceptorIn = interceptorIn;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
        if (timeout >= 0 && producer != null)
            try {
                producer.setTimeToLive(timeout);
            } catch (Exception e) {
            }
    }

    public long getWarnTimeout() {
        return warnTimeout;
    }

    public void setWarnTimeout(long warnTimeout) {
        this.warnTimeout = warnTimeout;
    }

    //	public MessageProducer getProducer() {
    //		return producer;
    //	}

}
