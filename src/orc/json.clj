(ns orc.json
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
	    [orc.core :as core]
	    [orc.read :as orc-read]
            [orc.macro :refer [with-async-record-reader]]
            [taoensso.timbre :as timbre :refer [log trace debug info warn error fatal infof debugf]])
  (import [org.apache.hadoop.fs Path])
  (:gen-class))


(defn stream-metadata [^org.apache.orc.TypeDescription des
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

(defn start [conf ^java.net.URI src-path col-headers col-handlers byte-limit & {:keys [bat-size buf-size coll-type meta]
                                                                                :or {bat-size orc-read/batch-size
                                                                                     buf-size orc-read/buffer-size
                                                                                     coll-type :vector
                                                                                     meta stream-metadata}}]
    (debugf "BATCH_SIZE=%d" bat-size)
    (debugf "BUFFER_SIZE=%d" buf-size)
    (debugf "COLLECTION_TYPE=%s" coll-type)
    (let [out-ch (async/chan buf-size)
          rdr (orc-read/reader (Path. src-path) conf)
          des (orc-read/schema rdr)
          ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat (orc-read/batch des bat-size)
	  collect (core/collector coll-type)]
      (with-async-record-reader [rr (.rows rdr)]
        (info "Starting json streamer thread...")
        (try
          ;; First batch is special case. Get initial value for metadata
          ;; and header construction
          (debug "Reading batch 1")
          (.nextBatch rr bat)

	  ;; Send stream metadata
          (debug "Sending stream metadata")
          (async/>!! out-ch (meta des bat))

          (let [hdr-chunk (format "[%s" (json/write-str (col-headers bat)))
                first-chunk (jsonify (collect (col-handlers bat) bat))]
            (loop [i 1
                   j 2
                   total (.count bat)
                   byte-total (+ (byte-count hdr-chunk) (byte-count first-chunk))
                   acc (if (= "" first-chunk) [hdr-chunk] [hdr-chunk first-chunk])]
              (debugf "Reading batch %d" j)
              (if (.nextBatch rr bat)
                (let [maps (collect (col-handlers bat) bat)
                      chunk (jsonify maps)
                      n (+ (byte-count chunk) byte-total)]
	          (if (< n byte-limit)
                    (recur i (inc j) (+ total (.count bat)) n (conj acc chunk))
                    (if (async/>!! out-ch (payload i (prep i (conj acc chunk))))
                      (recur (inc i) (inc j) (+ total (.count bat)) 0 [])
                      (warn "Channel is closed; cannot write."))))
                (do
                  (infof "rows.count %d" total)
                  (async/>!! out-ch (payload i (prep i acc "]")))
                  (async/close! out-ch)))))
          (catch Exception e
            (async/close! out-ch)
            (throw e))
	  (finally
	    (info "Thread finished."))))
      out-ch))
