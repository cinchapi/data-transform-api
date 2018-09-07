# Changelog

#### Version 1.1.0
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
* Added the `Transformers#composeForEach` meta transformer for applying a collection of transformers to each item within a collection of values.
* Added the `Transfroemrs#forEach` meta transformer for applying a transformer to each item within a collection of values.

#### Version 1.0.0
* Initial Release