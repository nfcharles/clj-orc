(ns orc.json
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
	    [orc.core :as core]
	    [orc.read :as orc-read]
            [orc.macro :refer [with-async-record-reader]])
  (import [org.apache.hadoop.fs Path])
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
          ;; Chunks 2..n must be prepended with delimiter for proper
          ;; downstream reassembly.
          (format ",%s%s" data suffix)))
      suffix))
  ([i xs]
    (prep i xs "")))

(defn payload [i s]
  {:i i :chunk s})

(defn start-streamer
  ([conf ^java.net.URI src-path col-headers col-handlers byte-limit bat-size meta]
    (let [out (async/chan buffer-size)
          rdr (orc-read/reader (Path. src-path) conf)
          des (orc-read/schema rdr)
          bat (orc-read/batch des bat-size)]
      (with-async-record-reader [rr (.rows rdr)]
        (println "Starting json streamer thread...")
        (try
          ;; First batch is special case. Get initial value for metadata
          ;; and header construction
          (.nextBatch rr bat)

	  ;; Send stream metadata
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
            (async/close! out)
            (throw e))))
      out))
  ([conf ^java.net.URI src-path col-headers col-handlers byte-limit]
    (start-streamer conf src-path col-headers col-handlers byte-limit orc-read/batch-size default-meta))
  ([conf ^java.net.URI src-path col-headers col-handlers byte-limit bat-size]
    (start-streamer conf src-path col-headers col-handlers byte-limit bat-size default-meta)))
