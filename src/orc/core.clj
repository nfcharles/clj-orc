(ns orc.core
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
            [orc.macro :refer [with-async-record-reader]])
  (import [org.apache.orc OrcFile]
          [org.apache.hadoop.fs Path]
          [org.apache.hadoop.conf Configuration])
  (:gen-class))


(defn raw->trimmed [coll]
  (let [s (json/write-str coll)]
    (subs s 1 (dec (count s)))))

(def config-mapping
  (hash-map
    "fs.file.impl" "org.apache.hadoop.fs.LocalFileSystem"))

(defn orc-config [& ops]
  (let [conf (Configuration.)]
    (println "CONFIGURATION")
    (doseq [[k v] config-mapping]
      (println (format "%s=%s" k v))
      (.set conf k v))
    conf))

(defn get-col [^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat n]
  (nth (.cols bat) n))

(defn value [deser bat col-n row-n]
  (let [col (get-col bat col-n)]
    (deser col row-n)))

(defn row->map [col-config bat row-n]
  (loop [col-n 0
         col-conf col-config
         rcrd (transient {})]
    (if-let [conf (first col-conf)]
      (let [val (value (conf :fn) bat col-n row-n)]
        (recur (inc col-n) (rest col-conf) (assoc! rcrd col-n val)))
      (persistent! rcrd))))

(defn rows->map-list [col-config ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (let [n-rows (.count bat)]
    (loop [row 0
           rcrds []]
      (if (< row n-rows)
        (let [rec (row->map col-config bat row)]
          (recur (inc row) (conj rcrds rec)))
        rcrds))))

(defn bsize [^java.lang.String s]
  (count (.getBytes s)))

(defn hdr-info [col-headers bat]
  (let [hdr  (col-headers bat)
        ser  (json/write-str (col-headers bat))
	size (bsize ser)]
    [size ser]))

(defn prepare-part
  ([part-n xs suffix]
    (println "Preparing accumulated records")
    (if (> (count xs) 0)
      (let [data (clojure.string/join "," xs)]
        (if (= part-n 1)
	  (format "%s%s" data suffix)
	  ;; Need to splice data blocks (at X byte boundary) by separator char
          (format ",%s%s" data suffix)))
      suffix))
  ([part-n xs]
    (prepare-part part-n xs "")))

(defn reader ^org.apache.orc.Reader [path conf]
  (OrcFile/createReader path (OrcFile/readerOptions conf)))

(defn schema ^org.apache.orc.TypeDescription [^org.apache.orc.Reader rdr]
  (.getSchema rdr))

(defn batch ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch [^org.apache.orc.TypeDescription sch]
  (.createRowBatch sch))

(defn start-worker [pipe conf ^java.lang.String src-path col-headers col-handlers byte-limit]
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
	       part-n 1
               acc []]
          (if (.nextBatch rr bat)
	    (let [ser (raw->trimmed (rows->map-list (col-handlers bat) bat))
	          ^long size (bsize ser)
	          cur-size (+ size byte-total)]
	      (println (format "BATCH=%d SIZE=%d" bat-n size))
	      (if (= bat-n 1)
	        ;; Handle first batch which requires prepended header info
	        (let [[^long sz sr] (hdr-info col-headers bat)]
	          (if (< (+ sz cur-size) byte-limit)
		    (recur (+ sz cur-size) (inc bat-n) part-n (conj acc (format "[%s,%s" sr ser)))
		    (async/>!! pipe [part-n (format "[%s,%s" sr ser)])))
	         ;; General case, accumulate serialized rows
	        (if (< cur-size byte-limit)
	          (recur cur-size (inc bat-n) part-n (conj acc ser))
                  (if (async/>!! pipe [part-n (prepare-part part-n (conj acc ser))])
		    (recur 0 (inc bat-n) (inc part-n) [])
		    (println "PIPE IS CLOSED: CAN'T WRITE")))))
            (do
	      (async/>!! pipe [part-n (prepare-part part-n acc "]")])
	      (async/close! pipe))))
	(catch Exception e
	  (println "Error reading records")
	  (println e)
	  (async/close! pipe))))))
