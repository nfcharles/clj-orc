(ns orc.col
  (import [org.apache.hadoop.hive.ql.exec.vector ColumnVector
                                                 BytesColumnVector
                                                 LongColumnVector
                                                 DoubleColumnVector
                                                 StructColumnVector
                                                 MapColumnVector
                                                 ListColumnVector])
  (:gen-class))


(declare deser)

;;; Deserializer function interface:
;;;
;;;   (fn <map>arg1, <ColumnVector>arg2, <int>arg3)
;;;
;;; where,
;;;   arg1: map spec for complex types
;;;   arg2: ColumnVector class
;;;   arg3: row number

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
  [fields ^StructColumnVector col row-n]
  (loop [xs (map list fields (.fields col))
         acc []]
    (if-let [x (first xs)]
      (let [[field _col] x
            handler (deser (:type field))]
        #_(println (format "name=%s, handler=%s" (:name field) handler))
        (recur (rest xs) (conj acc (handler (:fields field) _col row-n))))
      acc)))

(defn parse-map
  [fields ^MapColumnVector col row-n]
  (let [k-handler          ((:key fields) deser)
        v-handler          ((:value fields) deser)
        ^ColumnVector kcol (.keys col)
        ^ColumnVector vcol (.values col)
        ^int map-len       (nth (.lengths col) row-n)
        ^int offset        (nth (.offsets col) row-n)
        limit              (+ offset map-len)]
    (loop [i   offset
           acc {}]
      (if (< i limit)
        (let []
          ;; key cannot be complex, hence fields value is nil
          (recur (inc i) (assoc acc (k-handler nil kcol i) (v-handler (:fields fields) vcol i))))
        acc))))

;; TODO: implement
(defn parse-list
  [fields ^ListColumnVector col row-n]
  (throw (java.lang.Exception. "parse-list not implememted!!")))

(def deser
  (hash-map
    :boolean parse-boolean
    :string  parse-string
    :float   parse-float
    :double  parse-double
    :int     parse-int
    :bigint  parse-bigint
    :struct  parse-struct
    :map     parse-map
    :list    parse-list))

(defn accum [acc name func]
  (conj acc {:name name :fn func}))

(defn handlers
  "Returns a list of maps where each map contains a col deserializer.

   Input is list of maps defining column names and types.
   e.g.
     {:name 'foo' :type :string}
     {:name 'bar' :type :int   }

   Returns list of column name / deserializer handlers keyed by
   :name and :fn respectively.
   e.g.
     {:name 'foo'  :fn parse-string}
     {:name 'bar'  :fn parse-int}"
  [col-types]
  (loop [types col-types
         acc []]
    (if-let [col-type (first types)]
      (if-let [handler (deser (:type col-type))]
        (recur (rest types) (accum acc (:name col-type) (partial handler (:fields col-type))))
        (throw (java.lang.Exception. (format "Unsupported column type: %s" col-type))))
      acc)))
