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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.base.Verify;
import com.cinchapi.common.collect.AnyMaps;
import com.cinchapi.common.collect.Association;
import com.cinchapi.common.reflect.Reflection;
import com.google.common.collect.ImmutableMap;

/**
 * A {@link Transformer} that uses a {@link ScriptEngine} compatible script to
 * transform data.
 * <p>
 * The provided script has access to the key and value that are candidates for
 * transformation. The key is available via a variable named {@code key} and
 * the value is available via a variable named {@code value}.
 * </p>
 * <p>
 * The last line of the script determines the return value of the
 * transformation. If an Object or Map-like value is returned from the script,
 * the same is returned from the {@link #transform(String, Object)} function.
 * Otherwise, scalar values are assumed to be transformations to the original
 * value. In those instances, the original key is preserved and the original
 * value is replaced by the script's returned value.
 * </p>
 *
 * @author Jeff Nelson
 */
public class ScriptedTransformer implements Transformer, Serializable {

    private static final long serialVersionUID = 1596575180656202243L;

    /**
     * Reference to the {@link ScriptEngineManager}.
     */
    private static final ScriptEngineManager sem = new ScriptEngineManager();

    /**
     * Return a builder that will create a {@link ScriptedTransformer} that uses
     * a javascript interpreter.
     * 
     * @return the builder
     */
    public static Builder usingJavascript() {
        return new JavascriptTransformerBuilder();
    }

    /**
     * The {@link ScriptEngine} that handles {@link #script} evaluation.
     */
    private transient final ScriptEngine engine;

    private final String engineName;

    /**
     * The script that is used for transformation.
     */
    private final String script;

    /**
     * Construct a new instance.
     * 
     * @param engine
     * @param script
     */
    private ScriptedTransformer(String engine, String script) {
        this.engineName = engine;
        this.script = script;
        this.engine = sem.getEngineByName(engine);
        Verify.that(engine != null, "Invalid script engine {}", engine);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> transform(String key, Object value) {
        Bindings bindings = new SimpleBindings(
                Association.of(ImmutableMap.of("key", key, "value", value)));
        try {
            Object result = engine.eval(script, bindings);
            if(result instanceof Map) {
                // The #result is probably actually an instance of JsObject
                // which has a toString of [Object object]. In order to get the
                // object's properties into a Java friendly Map format we must
                // make a copy.
                boolean isArray;
                try {
                    isArray = Reflection.call(result, "isArray");
                }
                catch (Exception e) {
                    isArray = false;
                }
                result = isArray ? ((Map<String, Object>) result).values()
                        : ImmutableMap.copyOf((Map<String, Object>) result);
            }
            if(result instanceof Map) {
                return (Map<String, Object>) result;
            }
            else {
                // If the script returns anything other than a Map-like object,
                // assume that the transformation only applies to the value and
                // preserve the original key.
                return AnyMaps.create(key, result);
            }
        }
        catch (ScriptException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Deserialize this object from the {@code in} stream.
     * 
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        // Read script
        short scriptBytesLength = in.readShort();
        byte[] scriptBytes = new byte[scriptBytesLength];
        in.read(scriptBytes);
        String script = new String(scriptBytes, StandardCharsets.UTF_8);
        Reflection.set("script", script, this);

        // Read engine
        short engineNameBytesLength = in.readShort();
        byte[] engineNameBytes = new byte[engineNameBytesLength];
        in.read(engineNameBytes);
        String engineName = new String(engineNameBytes, StandardCharsets.UTF_8);
        ScriptEngine engine = sem.getEngineByName(engineName);
        Reflection.set("engine", engine, this);
    }

    /**
     * Serialize this object to the {@code out} stream.
     * 
     * @param out
     * @throws IOException
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        byte[] scriptBytes = script.getBytes(StandardCharsets.UTF_8);
        byte[] engineNameBytes = engineName.toString()
                .getBytes(StandardCharsets.UTF_8);
        out.writeShort(scriptBytes.length);
        out.write(scriptBytes);
        out.writeShort(engineNameBytes.length);
        out.write(engineNameBytes);
    }

    /**
     * Base {@link Builder} for {@link ScriptedTransformer}s.
     *
     * @author Jeff Nelson
     */
    public static abstract class Builder {

        private final String engine;
        private final StringBuilder script = new StringBuilder();

        /**
         * Construct a new instance.
         * 
         * @param engine
         */
        protected Builder(String engine) {
            this.engine = engine;
        }

        /**
         * Build the transformer.
         * 
         * @return the {@link ScriptedTransformer}
         */
        public final ScriptedTransformer build() {
            return new ScriptedTransformer(engine, script.toString());
        }

        /**
         * Define a variable within the script
         * 
         * @param var
         * @param value
         * @return this
         */
        public Builder define(String var, String value) {
            String line = doDefine(var, value);
            interpret(line);
            return this;
        }

        /**
         * Add a line of logic to the script.
         * 
         * @param line
         * @return this
         */
        public Builder interpret(String line) {
            script.append(line).append(System.lineSeparator());
            return this;
        }

        /**
         * Set {@code var} equal to {@code value} in the script context.
         * 
         * @param var
         * @param value
         * @return this
         */
        protected abstract String doDefine(String var, String value);

    }

    /**
     * A {@link Builder} for Javascript based {@link ScriptedTransformer}s.
     *
     * @author Jeff Nelson
     */
    public static class JavascriptTransformerBuilder extends Builder {

        /**
         * Construct a new instance.
         */
        public JavascriptTransformerBuilder() {
            super("javascript");
            define("transformers", AnyStrings.format("Java.type('{}')",
                    Transformers.class.getName()));
        }

        @Override
        protected String doDefine(String var, String value) {
            return AnyStrings.format("{} = {};", var, value);
        }

    }

}
