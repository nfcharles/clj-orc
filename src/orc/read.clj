(ns orc.read
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
	    [orc.core :as core]
            [orc.macro :refer [with-async-record-reader]])
  (import [org.apache.orc OrcFile]
          [org.apache.hadoop.fs Path]
          [org.apache.hadoop.conf Configuration])
  (:gen-class))


(def default-mapping
  (hash-map
    "fs.file.impl" {:value "org.apache.hadoop.fs.LocalFileSystem"}))

(defn configure
  ([mapping]
    (let [conf (Configuration.)]
      (println ">> Configuration <<")
      (doseq [[k v] mapping]
        (let [type_ (v :type)
	      value (v :value)]
          (if (= type_ :private)
	    (println (format "%s=%s" k (apply str (repeat (count value) "*"))))
            (println (format "%s=%s" k value)))
          (.set conf k value)))
      conf))
   ([]
     (configure default-mapping)))

(defn raw->trimmed [coll]
  (let [s (json/write-str coll)]
    (subs s 1 (dec (count s)))))

(defn byte-count [^java.lang.String s]
  (count (.getBytes s)))

(defn hdr-info [col-headers bat]
  (let [hdr  (col-headers bat)
        ser  (json/write-str (col-headers bat))
        size (byte-count ser)]
    [size ser]))

(defn prep
  ([i xs suffix]
    (if (> (count xs) 0)
      (let [data (clojure.string/join "," xs)]
        (if (= i 1)
          (format "%s%s" data suffix)
          ;; Need to join data blocks (at X byte boundary) by separator char
          (format ",%s%s" data suffix)))
      suffix))
  ([i xs]
    (prep i xs "")))

(defn payload [i s]
  {:i i :obj s})

(defn reader ^org.apache.orc.Reader [path conf]
  (OrcFile/createReader path (OrcFile/readerOptions conf)))

(defn schema ^org.apache.orc.TypeDescription [^org.apache.orc.Reader rdr]
  (.getSchema rdr))

(defn batch ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch [^org.apache.orc.TypeDescription sch]
  (.createRowBatch sch))

(defn start-worker [pipe conf src-path col-headers col-handlers byte-limit]
  (let [rdr (reader (Path. src-path) conf)
        bat (batch (schema rdr))]
    (with-async-record-reader [rr (.rows rdr)]
      (println "Starting worker thread...")

      ;; Process first batch separately. Need first batch reference for special
      ;; header processing.  JSON field names are swapped for ordinal numbers
      ;; for memory optimization and hence, we need a special header record
      (try
        (loop [byte-total 0
               bat-n 1
               i 1
               acc []]
          (if (.nextBatch rr bat)
            (let [ser (raw->trimmed (core/rows->map-list (col-handlers bat) bat))
                  ^long size (byte-count ser)
                  cur-size (+ size byte-total)]
              (if (= bat-n 1)
                ;; Handle first batch which requires prepended header info
                (let [[^long sz sr] (hdr-info col-headers bat)]
                  (if (< (+ sz cur-size) byte-limit)
                    (recur (+ sz cur-size) (inc bat-n) i (conj acc (format "[%s,%s" sr ser)))
                    (async/>!! pipe (payload i (format "[%s,%s" sr ser)))))
                 ;; General case, accumulate serialized rows
                (if (< cur-size byte-limit)
                  (recur cur-size (inc bat-n) i (conj acc ser))
                  (if (async/>!! pipe (payload i (prep i (conj acc ser))))
                    (recur 0 (inc bat-n) (inc i) [])
                    (println "PIPE IS CLOSED: CAN'T WRITE")))))
            (do
              (async/>!! pipe (payload i (prep i acc "]")))
              (async/close! pipe))))
        (catch Exception e
          (println "Error reading records")
          (println e)
          (async/close! pipe))))))
