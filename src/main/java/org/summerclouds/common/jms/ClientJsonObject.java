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

import java.io.IOException;
import java.util.LinkedList;

import javax.jms.JMSException;

import org.summerclouds.common.core.json.SecurityTransformHelper;
import org.summerclouds.common.core.node.IProperties;
import org.summerclouds.common.core.tool.MJson;

import com.fasterxml.jackson.databind.JsonNode;

public class ClientJsonObject extends ClientJson {

    private SecurityTransformHelper helper = new SecurityTransformHelper(null, this.log());

    public ClientJsonObject(JmsDestination dest) {
        super(dest);
    }

    public void sendObjectOneWay(IProperties prop, Object... obj) throws JMSException, IOException {
        JsonNode json = MJson.pojoToJson(obj, helper);
        sendJsonOneWay(prop, json);
    }

    public RequestResult<Object> sendObject(IProperties prop, Object... obj)
            throws JMSException, IOException, IllegalAccessException {
        JsonNode json = MJson.pojoToJson(obj, helper);
        RequestResult<JsonNode> res = sendJson(prop, json);
        if (res == null) return null;

        Object o = MJson.jsonToPojo(res.getResult(), helper);

        return new RequestResult<Object>(o, res.getProperties());
    }

    @SuppressWarnings("unchecked")
    public RequestResult<Object>[] sendObjectBroadcast(IProperties prop, Object... obj)
            throws JMSException, IOException, IllegalAccessException {
        // ObjectNode json = MJson.createObjectNode();
        JsonNode json = MJson.pojoToJson(obj, helper);
        RequestResult<JsonNode>[] answers = sendJsonBroadcast(prop, json);

        LinkedList<RequestResult<Object>> out = new LinkedList<>();

        for (RequestResult<JsonNode> res : answers) {
            if (res == null) continue;

            Object o = MJson.jsonToPojo(res.getResult(), helper);

            out.add(new RequestResult<Object>(o, res.getProperties()));
        }

        return out.toArray(new RequestResult[out.size()]);
    }

    public SecurityTransformHelper getHelper() {
        return helper;
    }

    public void setHelper(SecurityTransformHelper helper) {
        this.helper = helper;
    }
}
