(ns orc.core
  (:require [taoensso.timbre :as logging]
            [taoensso.timbre.appenders.core :as appenders])
  (:gen-class))


(def timestamp-opts
  {:pattern  "yyyy-MM-dd HH:mm:ss" #_:iso8601
   :locale   :jvm-default #_(java.util.Locale. "en")
   :timezone :utc})

(defn configure-logging
  ([stream level blacklist]
    (logging/merge-config!
      { :level level
        :timestamp-opts timestamp-opts
        :ns-blacklist blacklist
        :appenders {
        :println (appenders/println-appender {:stream stream})}}))
  ([stream level]
    (configure-logging stream level [])))

(defn get-col
  "Return nth column"
  [^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat n]
  (nth (.cols bat) n))

(defn value
  "Returns column value"
  [deser bat col-n row-n]
  (let [col (get-col bat col-n)]
    (deser col row-n)))

(defn row->map
  "Return row values as map"
  [col-config bat row-n]
  (loop [col-n 0
         col-conf col-config
         rcrd (transient {})]
    (if-let [conf (first col-conf)]
      (let [val (value (:fn conf) bat col-n row-n)]
        (recur (inc col-n) (rest col-conf) (assoc! rcrd col-n val)))
      (persistent! rcrd))))

(defn row->vector
  "Returns row values as vector"
  [col-config bat row-n]
  (loop [col-n 0
         col-conf col-config
         rcrd (transient [])]
    (if-let [conf (first col-conf)]
      (let [val (value (:fn conf) bat col-n row-n)]
        #_(println conf)
        (recur (inc col-n) (rest col-conf) (conj! rcrd val)))
      (persistent! rcrd))))

(defn rows->maps
  "Returns rows as list of maps"
  [col-config ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (let [n-rows (.count bat)]
    (loop [row 0
           rcrds []]
      (if (< row n-rows)
        (let [rec (row->map col-config bat row)]
          (recur (inc row) (conj rcrds rec)))
        rcrds))))

(defn rows->vectors
  "Returns rows as list of vectors"
  [col-config ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (let [n-rows (.count bat)]
    (loop [row 0
           rcrds []]
      (if (< row n-rows)
        (let [rec (row->vector col-config bat row)]
          (recur (inc row) (conj rcrds rec)))
        rcrds))))

(defn collector
  "Returns row aggregator"
  [coll-type]
  (case coll-type
    :vector rows->vectors
    :map    rows->maps
    (throw (java.lang.Exception. (format "Unsupported collection type: %s" coll-type)))))
