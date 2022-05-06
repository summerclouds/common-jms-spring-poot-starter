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
package org.summerclouds.common.jms.util;

import java.util.Enumeration;

import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.summerclouds.common.core.tool.MString;

public class MessageStringifier {

    private Message msg;

    public MessageStringifier(Message msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        if (msg == null) return "null";
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("\n=== JMS Message: ")
                    .append(msg.getClass().getSimpleName())
                    .append(" ===\n");
            if (msg.getJMSDestination() != null)
                sb.append(" Destination   : ").append(msg.getJMSDestination()).append('\n');
            if (msg.getJMSMessageID() != null)
                sb.append(" Message ID    : ").append(msg.getJMSMessageID()).append('\n');
            // sb.append("Destination   : ").append(msg.getJMSDestination()).append('\n');
            // sb.append("Type          : ").append(msg.getJMSType()).append('\n');
            if (msg.getJMSReplyTo() != null)
                sb.append(" Reply         : ").append(msg.getJMSReplyTo()).append('\n');
            // sb.append("Timestamp     : ").append(msg.getJMSTimestamp()).append('\n');
            if (msg.getJMSExpiration() != 0)
                sb.append(" Expiration    : ")
                        .append(msg.getJMSExpiration())
                        .append(" (")
                        .append(msg.getJMSExpiration() - msg.getJMSTimestamp())
                        .append(")\n");
            if (msg.getJMSCorrelationID() != null)
                sb.append(" Correlation ID: ").append(msg.getJMSCorrelationID()).append('\n');

            for (@SuppressWarnings("unchecked") Enumeration<String> e = msg.getPropertyNames();
                    e.hasMoreElements(); ) {
                String key = e.nextElement();
                Object val = msg.getObjectProperty(key);
                if (key.contains("assword")) val = "[***]";
                sb.append("  ").append(key).append('=').append(MString.toString(val)).append('\n');
            }

            if (msg instanceof MapMessage) {
                sb.append(" Map:\n");
                MapMessage m = (MapMessage) msg;
                for (@SuppressWarnings("unchecked") Enumeration<String> e = m.getMapNames();
                        e.hasMoreElements(); ) {
                    String key = e.nextElement();
                    Object val = ((MapMessage) msg).getObject(key);
                    if (key.contains("assword")) val = "[***]";
                    sb.append("   ")
                            .append(key)
                            .append('=')
                            .append(MString.toString(val))
                            .append('\n');
                }
            } else if (msg instanceof TextMessage) {
                sb.append(" Text: ").append(((TextMessage) msg).getText()).append('\n');
            } else if (msg instanceof BytesMessage) {
                sb.append(" Size: " + ((BytesMessage) msg).getBodyLength());
            } else if (msg instanceof ObjectMessage) {
                sb.append(" Object: ").append(((ObjectMessage) msg).getObject()).append('\n');
            }
        } catch (Throwable t) {
            sb.append(t);
        }
        return sb.toString();
    }
}
