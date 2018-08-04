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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Utility for transformations that take place in {@link Transformer
 * transformers}.
 *
 * @author Jeff Nelson
 */
public final class Transformations {

    /**
     * Return a transformation {@link Map} that maps {@code key} to a collection
     * containing {@code value}.
     * 
     * @param key
     * @param value
     * @return the transformation map
     */
    public static Map<String, Collection<Object>> singleKeyValuePair(String key,
            Object value) {
        return ImmutableMap.of(key, ImmutableList.of(value));
    }
    
    public static Map<String, Collection<Object>> singleKeyToMultiValues(String key, Object...values) {
        return ImmutableMap.of(key, Arrays.asList(values));
    }
    
    public static Map<String, Collection<Object>> none(){
        return null;
    }

    private Transformations() {/* no-init */ }

}
