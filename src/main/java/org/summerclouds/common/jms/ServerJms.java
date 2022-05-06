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

import java.util.LinkedList;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.security.auth.Subject;

import org.apache.activemq.ActiveMQSession;
import org.springframework.core.metrics.StartupStep.Tags;
import org.summerclouds.common.core.M;
import org.summerclouds.common.core.cfg.CfgBoolean;
import org.summerclouds.common.core.cfg.CfgInt;
import org.summerclouds.common.core.cfg.CfgLong;
import org.summerclouds.common.core.cfg.CfgString;
import org.summerclouds.common.core.security.ISubject;
import org.summerclouds.common.core.security.ISubjectEnvironment;
import org.summerclouds.common.core.tool.MPeriod;
import org.summerclouds.common.core.tool.MSecurity;
import org.summerclouds.common.core.tool.MThread;
import org.summerclouds.common.core.tool.MTracing;
import org.summerclouds.common.core.util.ThreadPool;

import de.mhus.lib.core.MThreadPool;
import de.mhus.lib.core.aaa.Aaa;
import de.mhus.lib.core.aaa.SubjectEnvironment;
import de.mhus.lib.core.logging.ITracer;
import io.opentracing.Span;
import io.opentracing.SpanContext;

public abstract class ServerJms extends JmsChannel implements MessageListener {

    public enum STRATEGY {
        DROP,
        JSON_ERROR,
        QUEUE,
        WAIT
    };

    private static final String JOB_PREFIX = "JMSJOB:";
    private static final String LISTENER_PREFIX = "JMSLISTENER:";
    private static volatile int usedThreads = 0;
    private static CfgInt CFG_MAX_THREAD_COUNT = new CfgInt(ServerJms.class, "maxThreadCount", -1);
    private static CfgLong maxThreadCountTimeout =
            new CfgLong(ServerJms.class, "maxThreadCountTimeout", 10000l);
    private static CfgLong inactivityTimeout =
            new CfgLong(ServerJms.class, "inactivityTimeout", MPeriod.HOUR_IN_MILLISECONDS);

    private static CfgString CFG_TRACE_ACTIVE =
            new CfgString(ServerJms.class, "traceActivation", "");
    private static CfgBoolean CFG_THREAD_POOL =
            new CfgBoolean(ServerJms.class, "threadPool", false);

    public ServerJms(JmsDestination dest) {
        super(dest);
    }

    MessageConsumer consumer;

    private MessageProducer replyProducer;
    private JmsInterceptor interceptorIn;

    private JmsInterceptor interceptorOut;

    private boolean fork = true;
    private long lastActivity = System.currentTimeMillis();
    private int maxThreadCount = -2;
    private STRATEGY overloadStrategy = STRATEGY.WAIT;
    private LinkedList<Message> backlog;

    @Override
    public synchronized void open() throws JMSException {
        if (isClosed()) throw new JMSException("server closed");
        if (consumer == null || getSession() == null) {
            lastActivity = System.currentTimeMillis();
            dest.open();
            if (dest.getConnection() == null || dest.getConnection().getSession() == null)
                throw new JMSException("connection offline");
            log().i("consume", dest);
            consumer = dest.getConnection().getSession().createConsumer(dest.getDestination());
            consumer.setMessageListener(this);
            onOpen();
        }
    }

    /** The method is called after the open was successful. */
    protected void onOpen() {}

    /** The method is called after the reset operation. */
    protected void onReset() {}

    public synchronized void openAnswer() throws JMSException {
        if (replyProducer == null || getSession() == null) {
            open();
            replyProducer = dest.getSession().createProducer(null);
        }
    }

    @Override
    public void reset() {
        if (isClosed()) return;
        lastActivity = System.currentTimeMillis();
        log().i("reset", dest);
        try {
            if (consumer != null) consumer.close();
        } catch (Throwable t) {
            log().d(t);
        }
        try {
            if (replyProducer != null) replyProducer.close();
        } catch (Throwable t) {
            log().d(t);
        }
        consumer = null;
        replyProducer = null;
        onReset();
    }

    public abstract void receivedOneWay(Message msg) throws JMSException;

    public abstract Message received(Message msg) throws JMSException;

    protected void sendAnswer(JmsContext rpcContext) throws JMSException {
        Message msg = rpcContext.getMessage();
        Message answer = rpcContext.getAnswer();
        openAnswer();
        if (answer == null) {
            answer =
                    createErrorAnswer(
                            "null"); // other side is waiting for an answer - send a null text
            rpcContext.setAnswer(answer);
        }
        if (interceptorOut != null) interceptorOut.prepare(rpcContext);
        answer.setJMSMessageID(createMessageId());
        answer.setJMSCorrelationID(msg.getJMSCorrelationID());
        try {
            answer.setStringProperty("_principal", rpcContext.getPrincipal());
        } catch (Throwable e) {
            log().t("set principal failed", getClass(), e);
        }
        final Message finalAnswer = answer;
        MTracing.get().inject((key, value) -> {
		        try {
		        	finalAnswer.setStringProperty(key, value);
		        } catch (Throwable t) {
		        	log().d(t);
		        }
        	}
        );

        replyProducer.send(
                msg.getJMSReplyTo(), answer, deliveryMode, getPriority(), getTimeToLive());
    }

    @Override
    public void onMessage(final Message message) {
        try {
            if (fork) {

                long mtc = getMaxThreadCount();
                if (mtc > 0 && getUsedThreads() > mtc) {

                    switch (overloadStrategy) {
                        case WAIT:
                            {
                                long timeout = getMaxThreadCountTimeout();
                                while (mtc > 0 && getUsedThreads() > mtc) {

                                    /*
                                    "AT100[232] de.mhus.lib.jms.ServerJms$1" Id=232 in BLOCKED on lock=de....aaa.AccessApiImpl@48781daa
                                         owned by AT92[224] de.mhus.lib.jms.ServerJms$1 Id=224
                                         ...
                                        at de.mhus.lib.karaf.jms.JmsDataChannelImpl.receivedOneWay(JmsDataChannelImpl.java:209)
                                        at de.mhus.lib.karaf.jms.ChannelWrapper.receivedOneWay(ChannelWrapper.java:20)
                                        at de.mhus.lib.jms.ServerJms.processMessage(ServerJms.java:182)
                                        at de.mhus.lib.jms.ServerJms$1.run(ServerJms.java:120)
                                        at de.mhus.lib.core.MThread$ThreadContainer.run(MThread.java:192)

                                    do not block jms driven threads !!! This will cause a deadlock

                                    				 */
                                    for (StackTraceElement element :
                                            Thread.currentThread().getStackTrace()) {
                                        if (element.getClassName()
                                                .equals(ServerJms.class.getCanonicalName())) {
                                            log().d(
                                                            "Overload: Too many JMS Threads ... ignore, it's a 'JMS to JMS' call",
                                                            getUsedThreads());
                                            break;
                                        }
                                    }

                                    log().d(
                                                    "Overload Too many JMS Threads ... waiting!",
                                                    getUsedThreads());
                                    MThread.sleep(100);
                                    timeout -= 100;
                                    if (timeout < 0) {
                                        log().w("Overload: timeout, drop", getUsedThreads());
                                        return;
                                    }
                                }
                            }
                            break;
                        case DROP:
                            log().w("Overload: drop JMS message");
                            return;
                        case JSON_ERROR:
                            try {
                                log().w("Overload: dorp and send error");
                                TextMessage answer = createErrorAnswer("overload");
                                log().d("errorAnswer", dest, answer);
                                JmsContext context = new JmsContext(message, null);
                                context.setAnswer(answer);
                                sendAnswer(context);
                            } catch (Throwable t) {
                                log().e(t);
                            }
                            return;
                        case QUEUE:
                            synchronized (this) {
                                if (backlog == null) backlog = new LinkedList<>();
                                backlog.add(message);
                            }
                            log().d("Overload: queue message", backlog.size());
                            return;
                    }
                }

                Runnable job =
                        new Runnable() {

                            @Override
                            public void run() {
                                try {
                                    log().t("process message", message);
                                    processMessage(message);

                                    // check backlog
                                    if (backlog != null) {
                                        while (true) {
                                            Message next = null;
                                            synchronized (this) {
                                                if (backlog.size() > 0)
                                                    next = backlog.removeFirst();
                                            }
                                            if (next == null) break;

                                            log().d("process queued message", next, backlog.size());
                                            processMessage(next);
                                        }
                                    }

                                } finally {
                                    decrementUsedThreads();
                                    log().t("<<< usedThreads", getUsedThreads());
                                }
                            }
                        };

                int nr = incrementUsedThreads();
                log().t(">>> usedThreads", getUsedThreads());
                String jobName = JOB_PREFIX + nr + " " + getJmsDestination().getName();
                if (CFG_THREAD_POOL.value()) new ThreadPool(job, jobName).start();
                else new MThread(job, jobName).start();
            } else {
                Thread.currentThread().setName(JOB_PREFIX + "- " + getJmsDestination().getName());
                processMessage(message);
            }
        } finally {
            Thread.currentThread().setName(LISTENER_PREFIX + getJmsDestination().getName());
        }
    }

    /** Overwrite this method to change default behavior. */
    protected void decrementUsedThreads() {
        usedThreads--;
    }

    /** Overwrite this method to change default behavior. */
    protected int incrementUsedThreads() {
        return usedThreads++;
    }

    /**
     * Overwrite this method to change default behavior.
     *
     * @return current used threads
     */
    public int getUsedThreads() {
        return usedThreads;
    }

    /**
     * Overwrite this method to change default behavior.
     *
     * @return max thread count or -1
     */
    public long getMaxThreadCount() {
        return maxThreadCount >= -2 ? maxThreadCount : CFG_MAX_THREAD_COUNT.value();
    }

    /**
     * Set max thread count. Set to -1 for unlimited, -2 to use global configuration
     *
     * @param maxThreadCount
     */
    public void setMaxThreadCount(int maxThreadCount) {
        this.maxThreadCount = maxThreadCount;
    }

    /**
     * Overwrite this method to change standard behavior.
     *
     * @return maxThreadCountTimeout.value()
     */
    protected long getMaxThreadCountTimeout() {
        return maxThreadCountTimeout.value();
    }

    public void processMessage(final Message message) {
        lastActivity = System.currentTimeMillis();

        MThread.cleanup();

        IScope scope = null;
        try {
            if (message != null) {
                try {
                    SpanContext parentSpanCtx =
                            ITracer.get()
                                    .tracer()
                                    .extract(Format.Builtin.TEXT_MAP, new TraceJmsMap(message));

                    if (parentSpanCtx == null) {
                        scope = ITracer.get().start(getName(), CFG_TRACE_ACTIVE.value());
                    } else if (parentSpanCtx != null) {
                        Span span =
                                ITracer.get()
                                        .tracer()
                                        .buildSpan(getName())
                                        .asChildOf(parentSpanCtx)
                                        .start();
                        scope = ITracer.get().activate(span);
                    }

                    if (scope != null) {
                        Tags.SPAN_KIND.set(ITracer.get().current(), Tags.SPAN_KIND_SERVER);
                    }

                } catch (Throwable t) {
                }
            }
            log().d("received", dest, message);

            String tokenStr = null;
            try {
                tokenStr = message.getStringProperty(M.PARAM_AUTH_TOKEN);
            } catch (Throwable e) {
                log().d("4", e);
                try {
                    if (message.getJMSReplyTo() != null) {
                        TextMessage answer = createErrorAnswer(e);
                        log().d("errorAnswer", dest, answer);
                        JmsContext context = new JmsContext(message, null);
                        context.setAnswer(answer);
                        sendAnswer(context);
                    }
                } catch (Throwable tt) {
                    log().w(tt);
                }
                return;
            }

            JmsContext context = new JmsContext(message, subject);
            try {
                if (interceptorIn != null) {
                    interceptorIn.begin(context);
                }
            } catch (Throwable t) {
                log().w(t);
                try {
                    if (message.getJMSReplyTo() != null) {
                        TextMessage answer = createErrorAnswer(t);
                        log().d("errorAnswer", dest, answer);
                        context.setAnswer(answer);
                        sendAnswer(context);
                    }
                } catch (Throwable tt) {
                    log().w(tt);
                }
                return;
            }

            try (ISubjectEnvironment access =  MSecurity.asSubject(tokenStr)) {
                if (message.getJMSReplyTo() != null) {
                    Message answer = null;
                    try {
                        answer = received(message);
                    } catch (JMSException t) {
                        throw t;
                    } catch (Throwable t) {
                        log().w(t);
                        answer = createErrorAnswer(t);
                    }
                    log().d("receivedAnswer", dest, answer);
                    context.setAnswer(answer);
                    sendAnswer(context);
                } else {
                    log().d("receivedOneWay", dest, message);
                    receivedOneWay(message);
                }
            } catch (SendNoAnswerException e) {
                log().d("0", "Suppress send of an answer", dest);
                log().t(e);
            } catch (InvalidDestinationException t) {
                log().w("1", Thread.currentThread().getName(), t);
            } catch (JMSException t) {
                reset();
                log().w("2", Thread.currentThread().getName(), t);
            } catch (Throwable t) {
                log().w("3", Thread.currentThread().getName(), t);
            } finally {
                if (interceptorIn != null) {
                    interceptorIn.end(context);
                }
            }

        } finally {
            if (scope != null) {
                scope.close();
            }
        }
    }

    protected TextMessage createErrorAnswer(Throwable t) throws JMSException {
        TextMessage ret = getSession().createTextMessage(null);
        if (t != null) ret.setStringProperty("_error", t.toString());
        return ret;
    }

    protected TextMessage createErrorAnswer(String error) throws JMSException {
        TextMessage ret = getSession().createTextMessage(null);
        if (error != null) ret.setStringProperty("_error", error);
        return ret;
    }

    @Override
    public void doBeat() {
        if (isClosed()) return;
        log().d("beat", dest);
        try {
            Session session = getSession();
            if (session instanceof ActiveMQSession && ((ActiveMQSession) getSession()).isClosed()) {
                log().w("reconnect because session is closed", getName());
                consumer = null;
            }
            open(); // try to reopen and re-listen

            if (inactivityTimeout.value() > 0
                    && MPeriod.isTimeOut(lastActivity, inactivityTimeout.value())) reset();

        } catch (JMSException e) {
            log().d(e);
        }
    }

    @Override
    public String getName() {
        return "openwire:/server" + dest.getName();
    }

    @Override
    public boolean isConnected() {
        if (consumer != null) {
            try {
                Message msg = consumer.receiveNoWait();
                if (msg != null) onMessage(msg);
            } catch (JMSException e) {
                // ok: javax.jms.IllegalStateException: Cannot synchronously receive a message when
                // a MessageListener is set
                if (!e.toString()
                        .equals(
                                "javax.jms.IllegalStateException: Cannot synchronously receive a message when a MessageListener is set")) {
                    try {
                        consumer.close();
                        consumer = null;
                    } catch (JMSException e1) {
                        log().d(getName(), e1);
                    }
                }
            }
        }
        return !(consumer == null || getSession() == null);
    }

    @Override
    public void checkConnection() {
        try {
            open();
        } catch (JMSException e) {
            log().d(e);
        }
    }

    public JmsInterceptor getInterceptorIn() {
        return interceptorIn;
    }

    public void setInterceptorIn(JmsInterceptor interceptor) {
        this.interceptorIn = interceptor;
    }

    public JmsInterceptor getInterceptorOut() {
        return interceptorOut;
    }

    public void setInterceptorOut(JmsInterceptor interceptorOut) {
        this.interceptorOut = interceptorOut;
    }

    public boolean isFork() {
        return fork;
    }

    /**
     * Set to true if a incoming message should be handled by a worker thread (asynchrony) or set to
     * false if the incoming message should be processed by the JMS thread (synchrony). The default
     * is 'true' because a synchrony mode will block all message handlers and causes dead locks.
     *
     * @param fork
     */
    public void setFork(boolean fork) {
        this.fork = fork;
    }

    public long getLastActivity() {
        return lastActivity;
    }

    public STRATEGY getOverloadStrategy() {
        return overloadStrategy;
    }

    public void setOverloadStrategy(STRATEGY overloadStrategy) {
        this.overloadStrategy = overloadStrategy;
    }
}
