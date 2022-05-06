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

import javax.jms.Message;

import org.summerclouds.common.core.node.IProperties;
import org.summerclouds.common.core.node.MProperties;
import org.summerclouds.common.core.security.ISubject;
import org.summerclouds.common.core.tool.MSecurity;

public class JmsContext {

    private Message message;
    private MProperties properties;
    private Message answer;
    private ISubject subject;

    public JmsContext(Message message, ISubject subject) {
        this.message = message;
        this.subject = subject;
    }

    public Message getMessage() {
        return message;
    }

    public IProperties getProperties() {
        if (properties == null) properties = new MProperties();
        return properties;
    }

    public Message getAnswer() {
        return answer;
    }

    public void setAnswer(Message answer) {
        this.answer = answer;
    }

    public ISubject getSubject() {
        return subject;
    }

    public String getPrincipal() {
        return subject == null ? MSecurity.GUEST : String.valueOf(subject.getPrincipal());
    }
}
