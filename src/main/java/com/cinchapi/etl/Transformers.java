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

import java.util.Arrays;
import java.util.Map;

import com.cinchapi.common.base.validate.Check;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.SplitOption;
import com.cinchapi.concourse.util.StringSplitter;
import com.cinchapi.concourse.util.Strings;
import com.google.common.base.CaseFormat;

/**
 * Utilities for import {@link Transformer} objects.
 * 
 * @author Jeff Nelson
 */
public final class Transformers {

    /**
     * Return a {@link CompositeTransformer} that invokes each of the
     * {@code transformers} in order.
     * 
     * @param transformers the {@link Transformer transformers} to invoke
     * @return a {@link CompositeTransformer}
     */
    public static Transformer compose(Transformer... transformers) {
        return new CompositeTransformer(Arrays.asList(transformers));
    }

    /**
     * Return a {@link Transformer} that converts keys {@code from} one case
     * format {@code to} another one.
     * 
     * @param from the original {@link CaseFormat}
     * @param to the desired {@link CaseFormat}
     * @return the {@link Transformer}
     */
    public static Transformer keyCaseFormat(CaseFormat from, CaseFormat to) {
        return (key, value) -> {
            key = from.to(to, key);
            return Transformations.singleKeyValuePair(key, value);
        };
    }

    /**
     * Return a {@link Transformer} that replaces all the character keys in the
     * {@code replacements} mapping with their associated character values in
     * each data key.
     * 
     * @param replacements a mapping from characters to another character with
     *            which it should be replaced
     * @return the {@link Transformer}
     */
    public static Transformer keyReplaceChars(
            Map<Character, Character> replacements) {
        return (key, value) -> {
            char[] chars = key.toCharArray();
            boolean modified = false;
            for (int i = 0; i < chars.length; ++i) {
                Character replacement = replacements.get(chars[i]);
                if(replacement != null) {
                    chars[i] = replacement;
                    modified = true;
                }
            }
            return modified
                    ? Transformations.singleKeyValuePair(new String(chars),
                            value)
                    : null;
        };
    }

    /**
     * Return a {@link Transformer} that strips invalid characters from the key.
     * Some of those characters are replaced with valid stand-ins.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer keyStripInvalidChars(Check<Character> validator) {
        return (key, value) -> {
            StringBuilder sb = new StringBuilder();
            boolean modified = false;
            for (char c : key.toCharArray()) {
                if(validator.passes(c)) {
                    sb.append(c);
                }
                else {
                    modified = true;
                }
            }
            return modified
                    ? Transformations.singleKeyValuePair(sb.toString(), value)
                    : null;
        };
    }

    /**
     * Return a {@link Transformer} that converts all the characters in a key to
     * lowercase. No other modifications to the key are made.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer keyToLowerCase() {
        return (key, value) -> Transformations
                .singleKeyValuePair(key.toLowerCase(), value);
    }

    /**
     * Return a {@link Transformer} that will strip single and double quotes
     * from the beginning and end of both the key and value.
     */
    public static Transformer keyValueStripQuotes() {
        return (key, value) -> {
            boolean modified = false;
            if(Strings.isWithinQuotes(key)) {
                key = key.substring(1, key.length() - 1);
                modified = true;
            }
            if(value instanceof String
                    && Strings.isWithinQuotes((String) value)) {
                String str = (String) value;
                value = str.substring(1, str.length() - 1);
                modified = true;
            }
            return modified ? Transformations.singleKeyValuePair(key, value)
                    : null;
        };
    }

    /**
     * Return a {@link Transformer} that does not perform any key or value
     * transformations.
     * 
     * @return a no-op {@link Transformer}
     */
    public static Transformer noOp() {
        return (key, value) -> null;
    }

    /**
     * Return a {@link Transformer} that splits a String value into multiple
     * strings that are all mapped from the original key.
     * 
     * @param delimiter the character on which to split the String
     * @param options the optional {@link SplitOption SplitOptions}
     * @return the transformer
     */
    public static Transformer valueSplitOnDelimiter(char delimiter,
            SplitOption... options) {
        return (key, value) -> {
            if(value instanceof String) {
                StringSplitter splitter = new StringSplitter((String) value,
                        delimiter, options);
                Object[] values = splitter.toArray();
                return values.length > 1
                        ? Transformations.singleKeyToMultiValues(key, values)
                        : null;
            }
            else {
                return null;
            }
        };
    }

    /**
     * Transform a string value to the proper java type using
     * {@link Convert#stringToJava(String)}.
     * 
     * @return the transformer
     */
    public static Transformer valueStringToJava() {
        return (key, value) -> {
            if(value instanceof String) {
                Object converted = Convert.stringToJava((String) value);
                return !converted.equals(value)
                        ? Transformations.singleKeyValuePair(key, converted)
                        : null;
            }
            else {
                return null;
            }
        };
    }

    private Transformers() {/* no init */}

}
