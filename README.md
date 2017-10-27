# clj-orc

clj-orc is a library for translating ORC files into json represenation.

## Installation

Fork repo and build project via lein.

```bash
lein uberjar
```

## Configuration

Field/Type mappings are required for each ORC data representation.  They are used to create column readers
responsible for data deserialization.  See example below:

### Column handlers
```clojure
(ns examples.fields
  (:require [orc.col :as col])
  (:gen-class))

(def foo
  (list
    {:name "x" :type "int" }
    {:name "y" :type "int" }))

(defn column-handlers [^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (orc-col/handlers foo))

;; Header records are used for memory optimization.  :map collection types use ordinal
;; values mapped to their corresponding field names.
;; e.g.
;; [
;;   {
;;     "0" : "field1",
;;     "1" : "field2",
;;     "2" : "field3"
;;   },
;;   {
;;     "0" : "value1",
;;     "1" : "value2",
;;     "2" : "value3"
;;   }
;; ]
;;
;; :vector collection type header records are a list of column names
;; e.g.
;;
;; [
;;  [ "field1", "field2", "field3"],
;;  [ "value1", "value2", "value3"]
;; ]
;;

(defn map-hdr-reducer [acc item]
  (assoc acc (item 0) ((item 1) :name)))

(defn vector-hdr-reducer [acc item]
  (conj acc (item :name)))

(defn column-headers [coll-type ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (case coll-type
    :map    (reduce map-hdr-reducer {} (map vector (range (count foo)) foo))
    :vector (reduce vector-hdr-reducer [] foo)
    (throw (java.lang.Exception. (format "Unsupported collection type: %s" coll-type)))))
```

### Hadoop
Hadoop configuration is optional.  Default configuration assumes a local filesystem for reading
ORC.  The following example demonstrates configuring the reader for remote reading - Amazon S3.

```clojure
(ns examples.config
  (:require [orc.read :as orc-read])
  (:gen-class))

(def s3-resource-mapping
  (hash-map
    "fs.file.impl" {:value "org.apache.hadoop.fs.s3native.NativeS3FileSystem"}
    "fs.s3n.awsAccessKeyId" {:value access-key-id :type :private}
    "fs.s3n.awsSecretAccessKey" {:value secret-key :type :private}))

(orc-read/configure s3-resource-mapping)
```
```:private``` configuration values are obfuscated during logging.

## Examples
### Low level batch translation
#### Use orc core methods to convert ```VectorizedRowBatch``` to list of maps
```clojure
(ns examples.driver
  (:require [orc.core :as orc-core]
            [example.fields :as fields])
  (:gen-class))

(loop [acc []]
  (if (.nextBatch reader batch)
    (recur conj acc (orc-core/rows->maps (fields/column-handlers batch) batch))
    acc))
```

### Multithreaded Processing
#### Use orc read worker to concurrently read and process ORC data (into clojure maps)
```clojure
(ns examples.driver
  (:require [orc.read :as orc-read]
            [example.fields :as fields])
  (:gen-class))

;; start method coll-type parameter defaults to :vector
(let [ch (orc-read/start conf uri (partial fields/column-headers :map) fields/column-handlers batch-size buffer-size :map)]
  (loop [acc []]
    (if-let [res (async/<!! ch)]
      (do
        ;; where result is list of hash-maps
        ;; [{'col_1' 'foo'
        ;;   'col_2' 'bar'
        ;;   'col_n' 'baz'},
        ;;  ...]
        (conj acc (process res))
        (recur))
      acc)))
```

```batch-size``` sets number of rows per ORC batch.

```buffer-size``` sets number of ORC batches queued into memory.

```coll-type``` can be either ```:vector``` or ```:map``` and determines the collection type of each json record.

```meta``` is a 2-arity function that takes ```TypeDescrition``` and ```VectorizedRowBatch``` objects as arguments.
The return value is the first value in the output stream. If no function is provided a default function will provide a default value.

#### Use orc json streamer to concurrently stream and process json chunks (records are json lists)
```clojure
(ns example.driver
  (:require [orc.json :as orc-json]
            [example.fields :as fields])
  (:gen-class))

;; start method coll-type defaults to :vector
(let [ch (orc-json/start conf uri (partial fields/column-headers :vector) fields/column-handlers byte-limit batch-size)]
  ;; First value from stream is stream metadata
  (println (async/<!! ch))
  (loop []
    (if-let [chunk (async/<!! ch)]
      (let [ret (process chunk)]
        (recur)))))
```
The last four arguments of the json streamer are optional.

```batch-size``` sets number of rows per ORC batch.

```buffer-size``` sets number of ORC batches queued into memory.

```coll-type``` can be either ```:vector``` or ```:map``` and determines the collection type of each json record.

```meta``` is a 2-arity function that takes ```TypeDescrition``` and ```VectorizedRowBatch``` objects as arguments.
The return value is the first value in the output stream. If no function is provided a default function will provide a default value.

## TODO
 * Complex type deserializers (with appropriate unit tests)

## License

Copyright © 2017 Navil Charles

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
