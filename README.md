# clj-orc

clj-orc is a library for converting ORC format files into json.

## Installation

Build project via lein

```bash
lein uberjar
```

## Examples

### Set up column handlers for deserializing data
```clojure
(ns examples.fields
  (:require [orc.deser :as deser])
  (:gen-class))


(def foo
  (list
    {:name "field1" :type "string" }
    {:name "field2" :type "int"    }
    {:name "field3" :type "int"    }))

(def bar
  (list
    {:name "field1" :type "int"   }
    {:name "field2" :type "strng" }))

(defn column-handlers [bat]
  (case (.numCols bat)
    3 (deser/col-handlers foo)
    2 (deser/col-handlers bar)
    (throw (java.lang.Exception "Unknown configuration"))))

(defn hdr-reducer [acc item]
  (assoc acc (item 0) ((item 1) :name)))

;; The first record of all output files are a header; field names in all
;; subsequent records are swapped for positional numbers for memory optimization.
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
    3 (reduce hdr-reducer (map vector (range 2) foo))
    2 (reduce hdr-reducer (map vector (range 2) bar))
    (throw (java.lang.Exception "Unknown configuration"))))
```

### Convert local ORC data to json
```clojure
(ns examples.runner
  (:require [examples.fields :as fields])
  (:require [orc.reader :as reader])  
  (:gen-class))

(def src-path "/tmp/test.orc")
(def dest-path-prefix "/tmp/translated/test")
(def wrt-thread-count 2)
(def file-limit 4096) ; increments of 1024
(orc->json src-path dest-path-prefix fields/column-headers fields/column-handlers wrt-thread-count file-limit)
```

### Convert local ORC data and push to remote location
```clojure
(ns examples.runner
  (:require [examples.fields :as fields])
  (:require [orc.reader :as reader])
  (:require [orc.macro :refer [with-tmp-workspace]])
  (:gen-class))

(def src-path "/tmp/test.orc")
(def wrt-thread-count 2)
(def file-limit 4096) ; increments of 1024
(with-tmp-workspace [out-path "examples"]
  (let [dest-path-prefix (format "%s/test" ws)]
    (orc->json src-path dest-path-prefix fields/column-headers fields/column-handlers wrt-thread-count file-limit)

    ; push files in workspace to remote location
```

## TODO
 * Memory optimizations
 * Multi threaded processing
 * Unit testing
 * Complex type deserializers

## License

Copyright © 2017 Navil Charles

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
