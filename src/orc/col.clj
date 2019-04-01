(ns orc.col
  (import [org.apache.hadoop.hive.ql.exec.vector ColumnVector
                                                 BytesColumnVector
                                                 LongColumnVector
                                                 DoubleColumnVector
                                                 StructColumnVector
                                                 MapColumnVector
                                                 ListColumnVector]
          [org.joda.time DateTime DateTimeZone]
          [org.joda.time.format DateTimeFormat])
  (:gen-class))


(declare deser)


;; ==========
;; -  Util  -
;; ==========

(def msec->sec 1000)
(def sec->min    60)
(def min->hour   60)
(def hour->day   24)
(def msec->day (* msec->sec sec->min min->hour hour->day))

(defn date->formatted-str
  [date & {:keys [fmt]
           :or {fmt "yyyy-MM-dd"}}]
  (let [dtf (DateTimeFormat/forPattern fmt)]
    (.print dtf date)))


;;; *** Deserializer Function Interface ***
;;;
;;;   (fn <map>arg1, <ColumnVector>arg2, <int>arg3)
;;;
;;; where,
;;;   arg1: map spec for complex types
;;;   arg2: ColumnVector class
;;;   arg3: row number


;; ==================
;; -  Scalar Types  -
;; ==================

(defn parse-bytes
  "Returns deserialized row value."
  [fields ^BytesColumnVector col row-n]
  (.toString col row-n))

(defn parse-long
  [fields ^LongColumnVector col row-n]
  (nth (.vector col) row-n))

(defn parse-long->date
  [fields ^LongColumnVector col row-n]
  (let [^long val (nth (.vector col) row-n)]
    (-> (DateTime. (* val msec->day))
        (.withZone DateTimeZone/UTC)
        (date->formatted-str))))

(defn parse-double
  [fields ^DoubleColumnVector col row-n]
  (let [val (nth (.vector col) row-n)]
    (if (Double/isNaN val)
      nil
      val)))


;; ====================
;; -  Compound Types  -
;; ====================


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
        ;; key cannot be compound type, hence `fields` value is nil
        (recur (inc i) (assoc acc (k-handler nil kcol i) (v-handler (:fields fields) vcol i)))
        acc))))

(defn parse-list
  [fields ^ListColumnVector col row-n]
  (let [v-handler          ((:type fields) deser)
        ^ColumnVector vcol (.child col)
        ^int list-len      (nth (.lengths col) row-n)
        ^int offset        (nth (.offsets col) row-n)
        limit              (+ offset list-len)]
    (loop [i offset
           acc []]
      (if (< i limit)
        (recur (inc i) (conj acc (v-handler (:fields fields) vcol i)))
        acc))))

;;; Nested compound types example definitions
;;;
;;; (def example
;;;   (list
;;;     {:name "foo" :type :map
;;;      :fields {:key :string :value :double}}
;;;     {:name "bar" :type :map
;;;      :fields {:key :string :value :struct
;;;               :fields [{:name "k1" :type :int}
;;;                        {:name "k2" :type :float}]}}
;;;     {:name "baz" :type :array
;;;      :fields {:type :int}}))
;;;
;;; (def example2
;;;   (list
;;;     {:name "foo" :type :array
;;;      :fields {:type :int}}
;;;     {:name "bar" :type :array
;;;      :fields {:type :map
;;;               :fields {:key :string :value :double}}}))
;;;
;;; (def example3
;;;   (list
;;;     {:name "foo" :type :array
;;;      :fields {:type :int}}
;;;     {:name "bar" :type :array
;;;      :fields {:type :map
;;;               :fields {:key :string :value :struct
;;;                        :fields [{:name "k1" :type :int}
;;;                                 {:name "k2" :type :boolean}]}}}))


(def deser
  (hash-map
    :array     parse-list
    :binary    parse-bytes
    :bigint    parse-long
    :boolean   parse-long
    :char      parse-bytes
    :date      parse-long->date
    :decimal   nil
    :double    parse-double
    :float     parse-double
    :int       parse-long
    :map       parse-map
    :smallint  parse-long
    :string    parse-bytes
    :struct    parse-struct
    :timestamp nil
    :tinyint   parse-long
    :uniontype nil
    :varchar   parse-bytes))


;; ===========
;; -  Utils  -
;; ===========

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
     {:name 'foo'  :fn parse-bytes}
     {:name 'bar'  :fn parse-long}"
  [col-types]
  (loop [types col-types
         acc []]
    (if-let [col-type (first types)]
      (if-let [handler (deser (:type col-type))]
        (recur (rest types) (accum acc (:name col-type) (partial handler (:fields col-type))))
        (throw (java.lang.Exception. (format "Unsupported column type: %s" col-type))))
      acc)))
