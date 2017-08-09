(ns orc.json
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
	    [orc.core :as core]
	    [orc.read :as orc-read]
            [orc.macro :refer [with-async-record-reader]])
  (import [org.apache.orc OrcFile]
          [org.apache.hadoop.fs Path]
          [org.apache.hadoop.conf Configuration])
  (:gen-class))


(def buffer-size 100)

(defn default-meta [^org.apache.orc.TypeDescription des
                    ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  "JSON Stream")

(defn jsonify [coll]
  (let [s (json/write-str coll)]
    (subs s 1 (dec (count s)))))

(defn byte-count [^java.lang.String s]
  (count (.getBytes s)))

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
  {:i i :chunk s})

(defn reader ^org.apache.orc.Reader [path conf]
  (OrcFile/createReader path (OrcFile/readerOptions conf)))

(defn schema ^org.apache.orc.TypeDescription [^org.apache.orc.Reader rdr]
  (.getSchema rdr))

(defn batch ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
  ([^org.apache.orc.TypeDescription sch bat-size]
    (.createRowBatch sch bat-size))
  ([^org.apache.orc.TypeDescription sch]
    (batch sch orc-read/batch-size)))

(defn start-streamer
  ([conf ^java.net.URI src-path col-headers col-handlers byte-limit bat-size meta]
    (let [out (async/chan buffer-size)
          rdr (reader (Path. src-path) conf)
          des (schema rdr)
          bat (batch des bat-size)]
      (with-async-record-reader [rr (.rows rdr)]
        (println "Starting json streamer thread...")
        (try
          ;; First batch is special case. Get initial value for metadata
	  ;; and header construction
	  (.nextBatch rr bat)

	  ;; Send metadata describing json stream
	  (async/>!! out (meta des bat))

          (let [hdr (col-headers bat)
                first-chunk (jsonify (core/rows->maps (col-handlers bat) bat))
                hdr-chunk (format "[%s" (json/write-str hdr))]
            (clojure.pprint/pprint hdr)
            (loop [i 1
	           byte-total (+ (byte-count hdr-chunk) (byte-count first-chunk))
                   acc [hdr-chunk first-chunk]]
              (if (.nextBatch rr bat)
                (let [chunk (jsonify (core/rows->maps (col-handlers bat) bat))
	              n (+ (byte-count chunk) byte-total)]
	          (if (< n byte-limit)
                    (recur i n (conj acc chunk))
                    (if (async/>!! out (payload i (prep i (conj acc chunk))))
                      (recur (inc i) 0 [])
                      (println "Channel is closed; cannot write."))))
                (do
                  (async/>!! out (payload i (prep i acc "]")))
                  (async/close! out)))))
          (catch Exception e
            (println "Error reading recording")
            (println e)
            (async/close! out))))
      out))
  ([conf ^java.net.URI src-path col-headers col-handlers byte-limit]
    (start-streamer conf src-path col-headers col-handlers byte-limit orc-read/batch-size default-meta))
  ([conf ^java.net.URI src-path col-headers col-handlers byte-limit bat-size]
    (start-streamer conf src-path col-headers col-handlers byte-limit bat-size default-meta)))
