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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.Enumeration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.summerclouds.common.core.cfg.CfgString;
import org.summerclouds.common.core.error.MException;
import org.summerclouds.common.core.log.Log;
import org.summerclouds.common.core.node.INode;
import org.summerclouds.common.core.node.IProperties;
import org.summerclouds.common.core.node.MNode;
import org.summerclouds.common.core.node.MProperties;
import org.summerclouds.common.core.operation.util.MapValue;
import org.summerclouds.common.core.pojo.MPojo;
import org.summerclouds.common.core.tool.MDate;
import org.summerclouds.common.core.tool.MJson;
import org.summerclouds.common.core.tool.MThread;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class MJms {
    private static final Log log = Log.getLog(MJms.class);
    private static final CfgString CFG_DEFAULT_CONNECTION =
            new CfgString(MJms.class, "defaultConnection", "default");

    public static void setProperties(IProperties prop, Message msg) throws JMSException {
        setProperties("", prop, msg);
    }

    public static void setProperties(String prefix, IProperties prop, Message msg)
            throws JMSException {
        if (prop == null || msg == null) return;
        for (Entry<String, Object> item : prop) {
            setProperty(prefix + item.getKey(), item.getValue(), msg);
        }
    }

    public static void setProperty(String name, Object value, Message msg) throws JMSException {
        if (value == null || msg == null || name == null) return;
        if (value instanceof String) msg.setStringProperty(name, (String) value);
        else if (value instanceof Boolean) msg.setBooleanProperty(name, (Boolean) value);
        else if (value instanceof Integer) msg.setIntProperty(name, (Integer) value);
        else if (value instanceof Long) msg.setLongProperty(name, (Long) value);
        else if (value instanceof Double) msg.setDoubleProperty(name, (Double) value);
        else if (value instanceof Byte) msg.setByteProperty(name, (Byte) value);
        else if (value instanceof Float) msg.setFloatProperty(name, (Float) value);
        else if (value instanceof Short) msg.setShortProperty(name, (Short) value);
        else if (value instanceof Date) msg.setStringProperty(name, MDate.toIso8601((Date) value));
        else msg.setStringProperty(name, String.valueOf(value));
    }

    public static IProperties getProperties(Message msg) throws JMSException {
        MProperties out = new MProperties();
        if (msg == null) return out;
        @SuppressWarnings("unchecked")
        Enumeration<String> enu = msg.getPropertyNames();
        while (enu.hasMoreElements()) {
            String name = enu.nextElement();
            out.setProperty(name, msg.getObjectProperty(name));
        }
        return out;
    }

    public static void setMapProperties(Map<?, ?> prop, MapMessage msg) throws JMSException {
        setMapProperties("", prop, msg);
    }

    public static void setMapProperties(String prefix, Map<?, ?> prop, MapMessage msg)
            throws JMSException {
        if (prop == null || msg == null) return;
        for (Entry<?, ?> item : prop.entrySet()) {
            setMapProperty(prefix + item.getKey(), item.getValue(), msg);
        }
    }

    public static void setMapProperty(String name, Object value, MapMessage msg)
            throws JMSException {
        if (msg == null || name == null) return;

        if (value == null) msg.setObject(name, null);
        else if (value instanceof String) msg.setString(name, (String) value);
        else if (value instanceof Boolean) msg.setBoolean(name, (Boolean) value);
        else if (value instanceof Integer) msg.setInt(name, (Integer) value);
        else if (value instanceof Long) msg.setLong(name, (Long) value);
        else if (value instanceof Double) msg.setDouble(name, (Double) value);
        else if (value instanceof Byte) msg.setByte(name, (Byte) value);
        else if (value instanceof Float) msg.setFloat(name, (Float) value);
        else if (value instanceof Short) msg.setShort(name, (Short) value);
        else if (value instanceof Date) msg.setString(name, MDate.toIso8601((Date) value));
        else msg.setString(name, String.valueOf(value));
    }

    public static IProperties getMapProperties(MapMessage msg) throws JMSException {
        MProperties out = new MProperties();
        if (msg == null) return out;
        @SuppressWarnings("unchecked")
        Enumeration<String> enu = msg.getMapNames();
        while (enu.hasMoreElements()) {
            String name = enu.nextElement();
            out.setProperty(name, msg.getObject(name));
        }
        return out;
    }

    public static INode getMapConfig(MapMessage msg) throws JMSException {
        MNode out = new MNode();
        if (msg == null) return out;
        @SuppressWarnings("unchecked")
        Enumeration<String> enu = msg.getMapNames();
        while (enu.hasMoreElements()) {
            String name = enu.nextElement();
            out.setProperty(name, msg.getObject(name));
        }
        return out;
    }

    public static Object toPrimitive(Object in) {
        if (in == null) return null;
        if (in.getClass().isPrimitive()) return in;
        if (in instanceof Date) return ((Date) in).getTime();
        return String.valueOf(in);
    }

    public static boolean isMapProperty(Object value) {
        return value == null
                || value.getClass().isPrimitive()
                || value instanceof String
                || value instanceof Boolean
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Double
                || value instanceof String
                || value instanceof Byte
                || value instanceof Float
                || value instanceof Short
                || value instanceof Date // use MDate
                || value instanceof UUID // to String
        ;
    }

    public static byte[] read(BytesMessage msg) throws JMSException {
        long len = msg.getBodyLength();
        byte[] bytes = new byte[(int) len];
        msg.readBytes(bytes);
        return bytes;
    }

    public static void read(BytesMessage msg, OutputStream os) throws JMSException, IOException {
        byte[] bytes = new byte[1024];
        while (true) {
            int i = msg.readBytes(bytes);
            if (i < 0) return;
            if (i == 0) MThread.sleep(200);
            else os.write(bytes, 0, i);
        }
    }

    public static void write(InputStream is, BytesMessage msg) throws JMSException, IOException {
        byte[] bytes = new byte[1024];
        while (true) {
            int i = is.read(bytes);
            if (i < 0) return;
            if (i == 0) MThread.sleep(200);
            else msg.writeBytes(bytes, 0, i);
        }
    }

    public static String getDefaultConnectionName() {
        return CFG_DEFAULT_CONNECTION.value();
    }

    public static Message toMessage(JmsObject jms, Object in) throws JMSException {

        Message ret = null;
        if (in == null) {
            ret = jms.createTextMessage(null);
            ret.setStringProperty("_encoding", "empty");
        } else if (in instanceof MapValue) {
            ret = jms.createMapMessage();
            ret.setStringProperty("_encoding", "map");
            MJms.setMapProperties((Map<?, ?>) ((MapValue) in).getValue(), (MapMessage) ret);
        } else if (in instanceof byte[]) {
            ret = jms.createBytesMessage();
            ret.setStringProperty("_encoding", "byte[]");
            ((BytesMessage) ret).writeBytes((byte[]) in);
        } else if (in instanceof String) {
            ret = jms.createTextMessage();
            ret.setStringProperty("_encoding", "text");
            ((TextMessage) ret).setText((String) in);
        } else if (in instanceof InputStream) {
            ret = jms.createBytesMessage();
            ret.setStringProperty("_encoding", "byte[]");
            InputStream is = (InputStream) in;
            long free = Runtime.getRuntime().freeMemory();
            if (free < 1024) free = 1024;
            if (free > 32768) free = 32768;
            byte[] buffer = new byte[(int) free];
            int i = 0;
            try {
                while ((i = is.read(buffer)) != -1) {
                    if (i == 0) MThread.sleep(100);
                    else {
                        ((BytesMessage) ret).writeBytes(buffer, 0, i);
                    }
                }
            } catch (Exception e) {
                log.d(e);
                throw new JMSException(e.toString());
            }
        } else if ((in instanceof INode) && !((INode) in).isProperties()) {
            ret = jms.createTextMessage();
            ret.setStringProperty("_encoding", "json");
            try {
                String val = INode.toPrettyJsonString((INode) in);
                ((TextMessage) ret).setText(val);
            } catch (MException e) {
                log.d(e);
                throw new JMSException(e.toString());
            }
        } else if (in instanceof IProperties) {
            ret = jms.createMapMessage();
            ret.setStringProperty("_encoding", "properties");
            MJms.setMapProperties((IProperties) in, (MapMessage) ret);
        } else if (in instanceof Map) {
            boolean primitive = true;
            for (Object value : ((Map<?, ?>) in).values())
                if (value != null
                        && !value.getClass().isPrimitive()
                        && !(value instanceof String)) {
                    primitive = false;
                    break;
                }
            if (primitive) {
                ret = jms.createMapMessage();
                ret.setStringProperty("_encoding", "map");
                MJms.setMapProperties((Map<?, ?>) in, (MapMessage) ret);
            } else {
                MNode cfg = new MNode();
                cfg.putMapToNode((Map<?, ?>) in);

                ret = jms.createTextMessage();
                ret.setStringProperty("_encoding", "json");
                try {
                    String val = INode.toPrettyJsonString(cfg);
                    ((TextMessage) ret).setText(val);
                } catch (MException e) {
                    log.d(e);
                    throw new JMSException(e.toString());
                }
            }
        } else {
            ret = jms.createTextMessage();
            ret.setStringProperty("_encoding", "json");
            try {
                ObjectNode json = MJson.createObjectNode();
                MPojo.pojoToJson(in, json);
                ((TextMessage) ret).setText(MJson.toPrettyString(json));
            } catch (IOException e) {
                log.d(e);
                throw new JMSException(e.getMessage());
            }
        }
        if (ret != null && in != null)
            ret.setStringProperty("_type", in.getClass().getCanonicalName());
        return ret;
    }

    public static Object toObject(Message msg) throws JMSException {
        if (msg == null) throw new NullPointerException("msg is null");
        if (msg instanceof MapMessage) {
            MapMessage mapMsg = (MapMessage) msg;
            return MJms.getMapProperties(mapMsg);
        } else if (msg instanceof TextMessage) {
            return ((TextMessage) msg).getText();
        } else if (msg instanceof BytesMessage) {
            long len = ((BytesMessage) msg).getBodyLength();
            byte[] bytes = new byte[(int) len];
            ((BytesMessage) msg).readBytes(bytes);
            return bytes;
        } else if (msg instanceof ObjectMessage) {
            return ((ObjectMessage) msg).getObject();
        }
        throw new JMSException("Unknown message type: " + msg.getClass().getCanonicalName());
    }

    public static INode toNode(Message msg) throws JMSException {
        if (msg == null) throw new NullPointerException("msg is null");
        if (msg instanceof MapMessage) {
            MapMessage mapMsg = (MapMessage) msg;
            return MJms.getMapConfig(mapMsg);
        } else if (msg instanceof TextMessage) {
            try {
                return INode.readNodeFromString(((TextMessage) msg).getText());
            } catch (MException e) {
                log.d(e);
                throw new JMSException(e.toString());
            }
        } else if (msg instanceof BytesMessage) {
            long len = ((BytesMessage) msg).getBodyLength();
            byte[] bytes = new byte[(int) len];
            ((BytesMessage) msg).readBytes(bytes);
            MNode ret = new MNode();
            ret.put(INode.NAMELESS_VALUE, bytes);
            return ret;
        } else if (msg instanceof ObjectMessage) {
            Serializable obj = ((ObjectMessage) msg).getObject();
            MNode ret = new MNode();
            ret.put(INode.NAMELESS_VALUE, obj);
            return ret;
        }
        throw new JMSException("Unknown message type: " + msg.getClass().getCanonicalName());
    }
}
