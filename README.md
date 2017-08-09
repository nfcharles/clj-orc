# clj-orc

clj-orc is a library for translating ORC files into json.

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


;; Schema 1
(def foo
  (list
    {:name "field1" :type "string" }
    {:name "field2" :type "int"    }
    {:name "field3" :type "int"    }))

;; Schema 2
(def bar
  (list
    {:name "field1" :type "int"   }
    {:name "field2" :type "strng" }))

(defn column-handlers [^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (case (.numCols bat)
    3 (col/handlers foo) ; list of deser functions for each column
    2 (col/handlers bar) ; list of deser functions for each column
    (throw (java.lang.Exception "Unknown configuration"))))

(defn hdr-reducer [acc item]
  (assoc acc (item 0) ((item 1) :name)))

;; Header records are used for memory optimization.
;; Field names are mapped to their ordinal values.
;; e.g.
;; [
;;   {
;;     "0" : "field1",
;;     "1" : "field2",
;;     "3" : "field3"
;;   },
;;   {
;;     "0" : "value1",
;;     "1" : "value2",
;;     "2" : "value3"
;;   }
;; ]
;;
(defn column-headers [bat]
  (case (.numCols bat)
    3 (reduce hdr-reducer (map vector (range 3) foo))
    2 (reduce hdr-reducer (map vector (range 2) bar))
    (throw (java.lang.Exception "Unknown configuration"))))
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
#### Use orc read worker to concurrently read and process ORC data
```clojure
(ns examples.driver
  (:require [orc.read :as orc-read]
            [example.fields :as fields])
  (:gen-class))

(let [ch (orc-read/start-worker conf uri fields/column-headers fields/column-handlers batch-size)]
  (loop [acc []]
    (if-let [batch (async/<!! ch)]
      (do
        ;; where batch is list of hash-maps
        ;; [{'col_1' 'foo'
        ;;   'col_2' 'bar'
        ;;   'col_n' 'baz'},
        ;;  ...]
        (conj acc (process batch))
        (recur))
      acc)))
```
```batch-size``` sets how many ORC records are batched into memory per iteration.

#### Use orc json streamer to concurrently stream and process json chunks
```clojure
(ns example.driver
  (:require [orc.json :as orc-json]
            [example.fields :as fields])
  (:gen-class))

(let [ch (orc-json/start-streamer conf uri fields/column-headers fields/column-handlers byte-limit batch-size)]
  ;; First value from stream is stream metadata
  (println (async/<!! ch))
  (loop []
    (if-let [chunk (async/<!! ch)]
      (process chunk)
      (recur))))
```
The last two arguments of the json streamer are optional.  ```byte-limit``` is the minimum number of bytes for each
json chunk.  ```meta``` is a 2-arity function that takes ```TypeDescrition``` and ```VectorizedRowBatch``` objects as
arguments. The return value is the first value in the output stream.  If no function is provided a default function
will provide a default value.

## TODO
 * Exhaustive unit testing
 * Complex type deserializers

## License

Copyright © 2017 Navil Charles

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
