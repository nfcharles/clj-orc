(ns orc.col
  (import [org.apache.hadoop.hive.ql.exec.vector ColumnVector
                                                 BytesColumnVector
                                                 LongColumnVector
                                                 DoubleColumnVector])
  (:gen-class))


;;; Deserializer function interface:
;;;
;;;   (fn <ColumnVector>arg1, <int>arg2)
;;;
;;; where,
;;;   arg1: ColumnVector classes
;;;   arg2: row number

(defn bool [^LongColumnVector col row-n]
  (nth (.vector col) row-n))

(defn string [^BytesColumnVector col row-n]
  "Returns deserialized row value."
  (let [buf (java.lang.StringBuilder.)]
    (.stringifyValue col buf row-n)
    buf))

(defn flt [^DoubleColumnVector col row-n]
  (let [val (nth (.vector col) row-n)]
    (if (Float/isNaN val)
      nil
      val)))

(defn dble [^DoubleColumnVector col row-n]
  (let [val (nth (.vector col) row-n)]
    (if (Double/isNaN val)
      nil
      val)))

(defn intg [^LongColumnVector col row-n]
  (nth (.vector col) row-n))

(defn long-intg [^LongColumnVector col row-n]
  (nth (.vector col) row-n))

(defn default [col row-n]
  "Returns deserialized row value"
  (nth (.vector col) row-n))


;;; TODO: implement remaining types - complex types.

(def type-mapper
  (hash-map
    "boolean" bool
    "string"  string
    "float"   flt
    "double"  dble
    "int"     intg
    "bigint"  long-intg))

(defn accum [acc name func]
  (conj acc {:name name :fn func}))

(defn handlers [col-types]
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
  (loop [ctypes col-types
         acc []]
    (if-let [ctype (first ctypes)]
      (if-let [handler (type-mapper (ctype :type))]
        (recur (rest ctypes) (accum acc (ctype :name) handler))
	(recur (rest ctypes) (accum acc (ctype :name) default)))
      acc)))
