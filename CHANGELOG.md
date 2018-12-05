# Changelog

#### Version 1.3.1 (TBD)
* Fixed some bugs related to serializing and deserializing some built-in `Transformers`.
* Added missing transformers to ensure values are handled as Strings.

#### Version 1.3.0 (November 23, 2018)
* Added the `ScriptedTransformer` framework. `ScriptedTransformer` is abstract class that can be extended to provide transformation logic via a script that is compatible with the Java script engine platform. Right now, javascript is the supported language for writing transformer scripts.
* Added support for Transformer serialization. The rules of serialization are:
  * All built-in transformers provided in the `Transformers` factory class are serializable.
  * `ScriptedTransformer`s are serializable.
  * A `CompositeTransformer` can be serialized if all of the composed transformers can be serailized.
  * Custom transformers that require serialization should be implemented using the `ScriptedTransformer` framework.
* Added `Transformer#serialize` and `Transformer#deserialize` static methods.
  

#### Version 1.2.0 (November 2, 2018)
* Deprecated and renamed a few `Transformer` factories for better consistency
  * `keyCaseFormat` is deprecated in favor of `keyConditionalConvertCaseFormat` or `keyEnsureCaseFormat`
  * `keyStripInvalidChars` is deprecated in favor of `keyRemoveInvalidChars`
  * `keyValueStripQuotes` is deprecated in favor of `keyValueRemoveQuotes`
  * `removeValuesThatAre` is deprecated in favor of `valueRemoveIf`
  * `valueSplitOnDelimiter` is deprecated in favor of `valueStringSplitOnDelimiter`
* Added some factories for common `Transformers`
  * `keyEnsureCaseFormat` can be used to conver ALL keys to a particular case format.
  * `keyWhitespaceToUnderscore` replace all whitespace characters with underscores in ALL keys
  * `keyRemoveWhitespace` removes all whitespace characters (no replacement) in ALL keys
  * `keyRemoveInvalidChars` (replacement for `keyStripInvalidChars`) has been overloaded to accept a collection of invalid characters in addition to a `Predicate` that determines if a character is invalid.
  * A version of `valueRemoveIfEmpty` that takes no arguments can be used to return a `Transformer` that removes values that match defeault definition of `Empty#ness`
  * Added versions of `valueAsBoolean`, `valueAsNumber`, `valueAsResolvableLinkInstruction`, `valueAsTag` and `valueAsTimestamp` that accept one or more keys to which the value transformation is limited. If no keys are provided, values for every key will be transformed.
* The version of `keyRemoveInvalidChars` that takes a `Predicate` expects the predicate to determine whether the character is `invalid`. whereas the `keyStripInvalidChars` method takes a `Check` that is expected to determmine if a character is valid.

#### Version 1.1.0 (October 18, 2018)
* Added an `Transformer#transform` method that takes a generic `Object` `value` parameter.
* Added a `Transformers#noOp` static factory that provides a `Transformer` that does not perform any key or value transformations.
* Added the following built-in `Transformers`:
  * `explode`
  * `keyMap` 
  * `valueAsBoolean`
  * `valueAsNumber`
  * `valueAsResolvableLinkInstruction`
  * `valueAsTag`
  * `valueStringToJava`
  * `valueAsTimestamp`
  * `valueNullifyIfEmpty`
* Added the `Transformers#composeForEach` meta transformer for applying a collection of transformers to each item within a sequence of values.
* Added the `Transfroemrs#forEach` meta transformer for applying a transformer to each item within a sequence of values.

#### Version 1.0.0
* Initial Release