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
package org.summerclouds.common.jms.ping;

import java.util.LinkedList;
import java.util.List;

import org.summerclouds.common.core.tool.MSystem;

public class PingImpl implements Ping {

    @Override
    public long time() {
        return System.currentTimeMillis();
    }

    @Override
    public List<String> ping(byte[] b) {
        LinkedList<String> out = new LinkedList<>();
        out.add(hostname());
        return out;
    }

    @Override
    public long timeDiff(long time) {
        return System.currentTimeMillis() - time;
    }

    @Override
    public String hostname() {
        return MSystem.getHostname();
    }
}
