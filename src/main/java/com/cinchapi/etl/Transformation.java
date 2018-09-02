/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.etl;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Contains utility for generations {@link Transformation} that can be returned
 * from {@link Transformer transformers}.
 * 
 * @author Jeff Nelson
 */
public final class Transformation {

    private Transformation() {/* no-init */}

    /**
     * Return a transformation containing a single key/value pair.
     * 
     * @param key
     * @param value
     * @return the transformation
     */
    public static Map<String, Object> to(String key, Object value) {
        return ImmutableMap.of(key, value);
    }

    /**
     * Return a transformation containing mapping from a single key to multiple
     * values.
     * 
     * @param key
     * @param values
     * @return the transformation
     */
    public static Map<String, Object> to(String key, Object[] values) {
        List<Object> vs = Lists.newArrayList();
        for (Object v : values) {
            vs.add(v);
        }
        return ImmutableMap.of(key, vs);
    }

    /**
     * Return a transformation containing mapping from a single key to multiple
     * values.
     * 
     * @param key
     * @param values
     * @return the transformation
     */
    public static Map<String, Object> to(String key, Object value,
            Object... values) {
        List<Object> vs = Lists.newArrayList();
        vs.add(value);
        for (Object v : values) {
            vs.add(v);
        }
        return ImmutableMap.of(key, vs);
    }

}
