# data-transform-api
The `data-transform-api` provides a common API for data transformations.

## Data Model
This API assumes that a data record is represented as a `Map<String, Object>`. While values can be primitive, container (e.g. `Map`, `Collection`) or custom types, some systems that rely on transformed data may only support subset of objects (i.e. `Concourse` does not support `Map` values).

## Trasformer
A `Transformer` is a functon that takes in a `Map<String, Object>` and returns a `Map<String, Object>` which contains transformations. If the `Transformer` doesn't modify the input data, it is customary for the function to return `null`.

### Pre-Defined Transformers
The `Transformers` class contains some useful transformers that are pre-defined.

### Composing Transformers
For maximum reusability, it is best to think of each `Transformer` as narrow function that only modifies one data characteristic. In doing so, it often becomes necessary to chain multiple transformers to fully transform a data set to its intended state. The `Transformers#compose` and `Transformers#composeForEach` factories will return a special `CompositeTransformer` that applies multiple transformers across a data set successively.

### Value Collections
The `Transformers#forEach` and `Transformers#composeForEach` factories provide meta transformers that will apply the transformation to each item within a value Collection. This should be used in the case of a datastore like `Concourse` that implicitly treats Collection values as containers of multiple primitive values.

