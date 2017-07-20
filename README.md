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
    {:name "f1" :type "string" }
    {:name "f2" :type "int"    }))

(def bar
  (list
    {:name "f1" :type "int"   }
    {:name "f2" :type "strng" }))

(defn column-handlers [val]
  (val 
    :foo (deser/col-handlers foo)
    :bar (deser/col-handlers bar)
    (throw (java.lang.Exception "Unknown configuration"))))
```

### Convert local ORC data to json
```clojure
(ns examples.runner
  (:require [examples.fields :as fields])
  (:require [orc.reader :as reader])  
  (:gen-class))

(def source-path "...")
(def out-path "...")
(orc->json source-path out-path (fields/column-handlers :foo))

; do something w/ files
```

### Convert local ORC data and push to remote location
```clojure
(ns examples.runner
  (:require [examples.fields :as fields])
  (:require [orc.reader :as reader])
  (:require [orc.macro :refer [with-tmp-workspace]])
  (:gen-class))

(def source-path "...")
(with-tmp-workspace [out-path "examples"]
  (orc->json source-path out-path (fields/column-handlers :foo))

  ; push files at 'out-path' to remote location
```

## TODO
 * Unit testing
 * Complex type deserializers

## License

Copyright © 2017 Navil Charles

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
