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

import java.util.LinkedList;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.summerclouds.common.core.tool.MSystem;
import org.summerclouds.common.jms.ClientJms;
import org.summerclouds.common.jms.JmsConnection;
import org.summerclouds.common.jms.JmsDestination;

public class HeartbeatSender extends ClientJms {

    public static final String TOPIC_NAME = "public.heartbeat";

    public HeartbeatSender(JmsConnection con) throws JMSException {
        super(con == null ? new JmsDestination(TOPIC_NAME, true) : con.createTopic(TOPIC_NAME));
    }

    public HeartbeatSender() throws JMSException {
        this(null);
    }

    public void sendHeartbeat(String cmd) {

        if (getSession() == null) {
            log().d("heartbeat has no session");
            reset();
            return;
        }

        //		try {
        //			getDestination().getConnection().doChannelBeat();
        //		} catch (Throwable e) {
        //			log().w("channel beat failed",e);
        //			return;
        //		}

        try {
            TextMessage msg =
                    getSession()
                            .createTextMessage(
                                    (cmd == null ? "ping" : cmd) + "," + MSystem.getAppIdent());
            Message[] ret = sendJmsBroadcast(msg);
            LinkedList<String> hosts = new LinkedList<>();
            for (Message m : ret) {
                if (m instanceof TextMessage) hosts.add(((TextMessage) m).getText());
            }
            log().d("hosts", hosts);
        } catch (Throwable e) {
            log().w(e);
        }
    }
}
