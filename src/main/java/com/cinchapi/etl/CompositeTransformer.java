/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
import java.util.Map.Entry;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * A {@link Transformer} that is composed of other {@link Transformer
 * Transformers}.
 * <p>
 * Each of the composing Transformers is applied in declaration order.
 * </p>
 * 
 * @author Jeff Nelson
 */
class CompositeTransformer implements Transformer {

    /**
     * An ordered list of the {@link Transformer transformers} that are applied
     * in the composite {@link #transform(String, String)} method
     */
    private final List<Transformer> transformers;

    /**
     * Construct a new instance.
     * 
     * @param transformers
     */
    public CompositeTransformer(List<Transformer> transformers) {
        Preconditions.checkArgument(!transformers.isEmpty());
        this.transformers = ImmutableList.copyOf(transformers);
    }

    @Override
    @Nullable
    public Entry<String, Object> transform(String key, Object value) {
        Entry<String, Object> transformed = null;
        for (Transformer transformer : transformers) {
            Entry<String, Object> current = transformer.transform(key, value);
            if(current != null) {
                transformed = current;
                key = transformed.getKey();
                value = transformed.getValue();
            }
        }
        return transformed;
    }

}
