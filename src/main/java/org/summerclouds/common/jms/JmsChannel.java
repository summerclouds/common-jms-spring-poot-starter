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

import java.util.UUID;

import javax.jms.DeliveryMode;
import javax.jms.Session;

import org.summerclouds.common.core.cfg.CfgLong;

public abstract class JmsChannel extends JmsObject {

    private static final CfgLong CFG_DEFAULT_TIMEOUT =
            new CfgLong(MJms.class, "msgTimeToLive", 60 * 60 * 1000l);
    protected JmsDestination dest;
    protected int deliveryMode = DeliveryMode.NON_PERSISTENT;
    protected int priority = 0; // default
    protected long timeToLive = CFG_DEFAULT_TIMEOUT.value();

    public JmsChannel(String destination, boolean destinationTopic) {
        dest = new JmsDestination(destination, destinationTopic);
    }

    public JmsChannel(JmsDestination dest) {
        this.dest = dest;
    }

    protected String createMessageId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public JmsDestination getJmsDestination() {
        return dest;
    }

    @Override
    public Session getSession() {
        return dest.getSession();
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public String toString() {
        return getName() + "/" + getClass().getSimpleName();
    }

    public abstract void doBeat();

    public abstract String getName();

    public void checkConnection() {}

    public void reset(JmsDestination dest) {
        this.dest = dest;
        reset();
    }

    public boolean isDeliveryModePersistent() {
        return deliveryMode == DeliveryMode.PERSISTENT;
    }

    public void setDeliveryModePersistent(boolean persistent) {
        this.deliveryMode = persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public long getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
    }
}
