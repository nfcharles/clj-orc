(ns orc.read
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
	    [orc.core :as core]
            [orc.macro :refer [with-async-record-reader]]
            [taoensso.timbre :as timbre :refer [log trace debug info warn error fatal infof debugf]])
  (import [org.apache.orc OrcFile]
          [org.apache.hadoop.fs Path]
          [org.apache.hadoop.conf Configuration])
  (:gen-class))


(def batch-size 1024)

(def buffer-size 100)

(def configuration-mapping
  (hash-map
    "fs.file.impl" {:value "org.apache.hadoop.fs.LocalFileSystem"}))

(defn stream-metadata
  [^org.apache.orc.TypeDescription des
   ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  "Read Stream")

(defn configure
  "Returns hash-map of hadoop configuration.  Input configuration has following
   interface:

   (hash-map
     'k1':  {:value 'v1' :type :private} ; credentials
     'k2':  {:value 'v2'})

  All configuration values are printed when config is applied. Values marked
  as private will be obfuscated."
  ([mapping]
    (let [conf (Configuration.)]
      (info "*** Configuration ***")
      (doseq [[k v] mapping]
        (let [type_ (:type v)
              value (:value v)]
          (if (= type_ :private)
            (infof "%s=%s" k (apply str (repeat (count value) "*")))
            (infof "%s=%s" k value))
          (.set conf k value)))
      conf))
   ([]
     (configure configuration-mapping)))

(defn reader
  ^org.apache.orc.Reader [path conf]
  (OrcFile/createReader path (OrcFile/readerOptions conf)))

(defn schema
  ^org.apache.orc.TypeDescription [^org.apache.orc.Reader rdr]
  (.getSchema rdr))

(defn batch ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
  ([^org.apache.orc.TypeDescription sch bat-size]
    (.createRowBatch sch bat-size))
  ([^org.apache.orc.TypeDescription sch]
    (batch sch batch-size)))

(defn start
  "Starts reader thread"
  [conf ^java.net.URI src-path col-headers col-handlers & {:keys [bat-size buf-size coll-type meta]
                                                           :or {bat-size batch-size
                                                                buf-size buffer-size
                                                                coll-type :vector
                                                                meta stream-metadata}}]
    (debugf "BATCH_SIZE=%d" bat-size)
    (debugf "BUFFER_SIZE=%d" buf-size)
    (debugf "COLLECTION_TYPE=%s" coll-type)
    (let [out-ch (async/chan buf-size)
          rdr (reader (Path. src-path) conf)
          des (schema rdr)
          ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat (batch des bat-size)
	  collect (core/collector coll-type)]
      (with-async-record-reader [rr (.rows rdr)]
        (info "Starting reader thread...")
        (try
          ;; First batch is special case. Get initial value for header construction
          (debug "Reading batch 1")
          (.nextBatch rr bat)

	  ;; Send stream metadata
          (debug "Sending stream metadata")
          (async/>!! out-ch (meta des bat))

          ;; Send first translated batch
          (debug "Sending stream header")
          (async/>!! out-ch (col-headers bat))
          (async/>!! out-ch {:i 1 :rows (collect (col-handlers bat) bat)})

          (loop [i 2
                 total (.count bat)]
            (debugf "Reading batch %d" i)
            (if (.nextBatch rr bat)
              (let [rows (collect (col-handlers bat) bat)]
                (if (async/>!! out-ch {:i i :rows rows})
                  (recur (inc i) (+ total (.count bat)))
                  (warn "Channel is closed; cannot write.")))
              (do
                (infof "rows.count %d" total)
                (async/close! out-ch))))
          (catch Exception e
            (async/close! out-ch)
            (throw e))
	  (finally
	    (info "Thread finished."))))
      out-ch))
