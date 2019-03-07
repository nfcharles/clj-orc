(ns orc.col
  (import [org.apache.hadoop.hive.ql.exec.vector ColumnVector
                                                 BytesColumnVector
                                                 LongColumnVector
                                                 DoubleColumnVector
                                                 StructColumnVector
                                                 MapColumnVector
                                                 ListColumnVector])
  (:gen-class))

(declare type-mapper)


;;; Deserializer function interface:
;;;
;;;   (fn <ColumnVector>arg1, <int>arg2)
;;;
;;; where,
;;;   arg1: ColumnVector classes
;;;   arg2: row number

(defn parse-boolean
  [fields ^LongColumnVector col row-n]
  (nth (.vector col) row-n))

(defn parse-string
  "Returns deserialized row value."
  [fields ^BytesColumnVector col row-n]
  (.toString col row-n))

(defn parse-float
  [fields ^DoubleColumnVector col row-n]
  (let [val (nth (.vector col) row-n)]
    (if (Float/isNaN val)
      nil
      val)))

(defn parse-double
  [fields ^DoubleColumnVector col row-n]
  (let [val (nth (.vector col) row-n)]
    (if (Double/isNaN val)
      nil
      val)))

(defn parse-int
  [fields ^LongColumnVector col row-n]
  (nth (.vector col) row-n))

(defn parse-bigint
  [fields ^LongColumnVector col row-n]
  (nth (.vector col) row-n))

(defn parse-struct
  [fields ^ColumnVector col row-n]
    (loop [xs (map list fields (.fields col))
           acc []]
      (if-let [x (first xs)]
        (let [[field _col] x
              handler (type-mapper (:type field))]
          #_(println (format "name=%s, handler=%s" (:name field) handler))
          (recur (rest xs) (conj acc (handler (:fields field) _col row-n))))
        acc)))

(defn default
  [col row-n]
  "Returns deserialized row value"
  (nth (.vector col) row-n))


;;; TODO: implement remaining types - complex types.

(def type-mapper
  (hash-map
    :boolean parse-boolean
    :string  parse-string
    :float   parse-float
    :double  parse-double
    :int     parse-int
    :bigint  parse-bigint
    :struct  parse-struct))

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
  (loop [types col-types
         acc []]
    (if-let [col-type (first types)]
      (if-let [handler (type-mapper (:type col-type))]
        (recur (rest types) (accum acc (:name col-type) (partial handler (:fields col-type))))
        (recur (rest types) (accum acc (:name col-type) default)))
      acc)))
