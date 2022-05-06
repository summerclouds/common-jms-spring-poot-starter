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
package org.summerclouds.common.jms.heartbeat;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.summerclouds.common.core.M;
import org.summerclouds.common.core.tool.MSystem;
import org.summerclouds.common.jms.JmsConnection;
import org.summerclouds.common.jms.JmsDestination;
import org.summerclouds.common.jms.ServerJms;

public class HeartbeatReceiver extends ServerJms {

    public HeartbeatReceiver(JmsConnection con) throws JMSException {
        super(
                con == null
                        ? new JmsDestination(HeartbeatSender.TOPIC_NAME, true)
                        : con.createTopic(HeartbeatSender.TOPIC_NAME));
    }

    public HeartbeatReceiver() throws JMSException {
        this(null);
    }

    @Override
    public void receivedOneWay(Message msg) throws JMSException {}

    @Override
    public Message received(Message msg) throws JMSException {
        String txt = "";
        if (msg instanceof TextMessage) txt = ((TextMessage) msg).getText();
        log().d("received", txt);
        HeartbeatListener listener = M.l(HeartbeatListener.class);
        if (listener != null) listener.heartbeatReceived(txt);
        TextMessage ret = getSession().createTextMessage(MSystem.getAppIdent());
        return ret;
    }
}
