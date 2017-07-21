(ns orc.deser
  (:gen-class))

;;; All deserializer functions expect ColumnVector classes as first
;;; argument and integer row number as second.


(defn string [col row-n]
  "Returns deserialized row value."
  ;; TODO: check repeating flag for ColumnVector
  (let [buf (java.lang.StringBuilder.)]
    (.stringifyValue col buf row-n)
    buf))

(defn flt [col row-n]
  (let [val (nth (.vector col) row-n)]
    (if (Float/isNaN val)
      Float/MIN_VALUE
      val)))

(defn dble [col row-n]
  (let [val (nth (.vector col) row-n)]
    (if (Double/isNaN val)
      Double/MIN_VALUE
      val)))

(defn default [col row-n]
  "Returns deserialized row value"
  ;; TODO: check repeating flag for ColumnVector
  (nth (.vector col) row-n))


;;; TODO: implement remaining types - complex types.

(def type-mapper
  (hash-map
    "string" string
    "float"  flt
    "double" dble))

(defn accum [acc name func]
  (conj acc {:name name :fn func}))

(defn col-handlers [col-types]
  "Returns a list of maps where each map contains a col deserializer.

   Input is list of maps defining column names and types.
   e.g.
     {:name 'foo' :type 'string' }
     {:name 'bar' :type 'int'    }

   Returns list of column name / deserializer handlers keyed by
   :name and :fn respectively.
   e.g.
     {:name 'foo'  :fn string  }
     {:name 'bar'  :fn default }"
  (loop [cts col-types
         acc []]
    (if-let [col-type (first cts)]
      (if-let [handler (type-mapper (col-type :type))]
        (recur (rest cts) (accum acc (col-type :name) handler))
	(recur (rest cts) (accum acc (col-type :name) default)))
      acc)))
