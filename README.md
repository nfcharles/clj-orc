# clj-orc

clj-orc is a library for reading ORC files. Low level streamers facilitate writing into arbitrary formats; alternatively, the json streamer can be used to write json files.

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
  (:require [orc.col])
  (:gen-class))

(def foo
  (list
    {:name "x" :type :int}
    {:name "y" :type :int}))

(def bar
  (list
    {:name "x" :type :int}
    {:name "y" :type :int}
    {:name "a" :type :map
      :fields {:key :string :value :double}}
    {:name "b" :type :struct
      :fields
       [{:name "foo" :type :string}
        {:name "bar" :type :string}
        {:name "baz" :type :string}]}))

(defn col-handlers [^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (orc.col/handlers foo))

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

(defn map-reducer [acc item]
  (assoc acc (item 0) ((item 1) :name)))

(defn vector-reducer [acc item]
  (conj acc (item :name)))

(defn col-headers [coll-type ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (case coll-type
    :map    (reduce map-reducer {} (map vector (range (count foo)) foo))
    :vector (reduce vector-reducer [] foo)
    (throw (java.lang.Exception. (format "Unsupported collection type: %s" coll-type)))))
```

### Hadoop
Hadoop configuration is optional.  Default configuration assumes a local filesystem for reading
ORC.  The following example demonstrates configuring the reader for remote reading - Amazon S3.

```clojure
(ns examples.config
  (:require [orc.read])
  (:gen-class))

(def s3-configuration
  (hash-map
    "fs.file.impl"      {:value "org.apache.hadoop.fs.s3a.S3AFileSystem"}
    "fs.s3a.access.key" {:value akey :type :private}
    "fs.s3a.secret.key" {:value skey :type :private}))

(orc-read/configure s3-configuration)

;; set path to remote URI
(def path (java.net.URI. "s3a://bucket/path/to/key"))

```
```:private``` configuration values are obfuscated during logging.

## Examples
### Low level batch translation
#### Use orc core methods to convert ```VectorizedRowBatch``` to list of maps
```clojure
(ns examples.driver
  (:require [orc.core]
            [example.fields :as fields])
  (:gen-class))

(loop [acc []]
  (if (.nextBatch reader batch)
    (recur conj acc (orc.core/rows->maps (fields/column-handlers batch) batch))
    acc))
```

### Multithreaded Processing
#### Use orc read worker to concurrently read and process ORC data (into clojure maps)
```clojure
(ns examples.driver
  (:require [orc.read]
            [example.fields :as flds])
  (:gen-class))

;; start method coll-type parameter defaults to :vector
(let [ch (orc.read/start conf uri (partial flds/col-headers :map) flds/col-handlers
                         :bat-size batch-size
                         :buf-size buffer-size
                         :coll-type :map)]
  ;; First value from stream is stream metadata
  (println (async/<!! ch))

  ;; Header record
  (println (async/<!! ch))

  (loop [acc []]
    (if-let [res (async/<!! ch)]
      ;; where result is list of hash-maps
      ;; [{'col_1' 'foo'
      ;;   'col_2' 'bar'
      ;;   'col_n' 'baz'},
      ;;  ...]
      (recur (conj acc (process res)))
      acc)))
```
The last four parameters of the read streamer are optional keyword arguments.

```:bat-size``` sets number of rows per ORC batch.

```:buf-size``` sets number of ORC batches queued into memory.

```:coll-type``` can be either ```:vector``` or ```:map``` and determines the collection type of each json record.

```:meta``` is a 2-arity function that takes ```TypeDescrition``` and ```VectorizedRowBatch``` objects as arguments.
The return value is the first value in the output stream. If no function is provided a default function will provide a default value.

#### Use orc json streamer to concurrently stream and process json chunks (records are json lists)
```clojure
(ns example.driver
  (:require [orc.json]
            [example.fields :as flds])
  (:gen-class))

;; start method coll-type defaults to :vector
(let [ch (orc.json/start conf uri (partial flds/col-headers :vector) flds/col-handlers byte-limit :bat-size batch-size)]
  ;; First value from stream is stream metadata
  (println (async/<!! ch))

  ;; Header record
  (println (async/<!! ch))

  (loop []
    (if-let [chunk (async/<!! ch)]
      (let [ret (process chunk)]
        (recur)))))
```
The last four parameters of the json streamer are optional keyword arguments.

```:bat-size``` sets number of rows per ORC batch.

```:buf-size``` sets number of ORC batches queued into memory.

```:coll-type``` can be either ```:vector``` or ```:map``` and determines the collection type of each json record.

```:meta``` is a 2-arity function that takes ```TypeDescrition``` and ```VectorizedRowBatch``` objects as arguments.
The return value is the first value in the output stream. If no function is provided a default function will provide a default value.

### Nested Type Definitions

The following examples illustrate deeply nested type configurations.

```clojure
(def example
  (list
    {:name "foo" :type :map
     :fields {:key :string :value :double}}
    {:name "bar" :type :map
     :fields {:key :string :value :struct
              :fields [{:name "k1" :type :int}
                       {:name "k2" :type :float}]}}
    {:name "baz" :type :array
     :fields {:type :int}}))

(def example2
  (list
    {:name "foo" :type :array
     :fields {:type :int}}
    {:name "bar" :type :array
     :fields {:type :map
              :fields {:key :string :value :double}}}))

(def example3
  (list
    {:name "foo" :type :array
     :fields {:type :int}}
    {:name "bar" :type :array
     :fields {:type :map
              :fields {:key :string :value :struct
                       :fields [{:name "k1" :type :int}
                                {:name "k2" :type :boolean}]}}}))
```

## Type Coverage

ORC Type |Implemented
---------|-----------
array    | x
binary   | x
bigint   | x
boolean  | x
char     | x
date     | x
decimal  |
double   | x
float    | x
int      | x
map      | x
smallint | x
string   | x
struct   | x
timestamp| x
tinyint  | x
union    |
varchar  | x


## License

Copyright © 2017 Navil Charles

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
