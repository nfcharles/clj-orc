(ns orc.read
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
	    [orc.core :as core]
            [orc.macro :refer [with-async-record-reader]]
            [taoensso.timbre :as timbre
                             :refer [log
                                     trace
                                     debug
                                     info
                                     warn
                                     error
                                     fatal
                                     report]])
  (import [org.apache.orc OrcFile]
          [org.apache.hadoop.fs Path]
          [org.apache.hadoop.conf Configuration])
  (:gen-class))


(def buffer-size 100)

(def batch-size 1024)

(def default-mapping
  (hash-map
    "fs.file.impl" {:value "org.apache.hadoop.fs.LocalFileSystem"}))

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
        (let [type_ (v :type)
	      value (v :value)]
          (if (= type_ :private)
            (info (format "%s=%s" k (apply str (repeat (count value) "*"))))
            (info (format "%s=%s" k value)))
          (.set conf k value)))
      conf))
   ([]
     (configure default-mapping)))

(defn reader ^org.apache.orc.Reader [path conf]
  (OrcFile/createReader path (OrcFile/readerOptions conf)))

(defn schema ^org.apache.orc.TypeDescription [^org.apache.orc.Reader rdr]
  (.getSchema rdr))

(defn batch ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
  ([^org.apache.orc.TypeDescription sch bat-size]
    (.createRowBatch sch bat-size))
  ([^org.apache.orc.TypeDescription sch]
    (batch sch batch-size)))

(defn start-worker
  ([conf ^java.net.URI src-path col-headers col-handlers bat-size]
    (let [out (async/chan buffer-size)
          rdr (reader (Path. src-path) conf)
	  des (schema rdr)
          ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat (batch des bat-size)]
      (with-async-record-reader [rr (.rows rdr)]
        (info "Starting worker thread...")
        (try
          ;; First batch is special case. Get initial value for header construction
          (.nextBatch rr bat)

          ;; Send first translated batch
          (async/>!! out (col-headers bat))
          (async/>!! out {:i 1 :rows (core/rows->maps (col-handlers bat) bat)})

          (loop [i 2
                 total (.count bat)]
            (if (.nextBatch rr bat)
              (let [rows (core/rows->maps (col-handlers bat) bat)]
                (if (async/>!! out {:i i :rows rows})
                  (recur (inc i) (+ total (.count bat)))
                  (warn "Channel is closed; cannot write.")))
              (do
                (info (format "rows.count=%d" total))
                (async/close! out))))
          (catch Exception e
            (async/close! out)
            (throw e))
	  (finally
	    (info "Thread finished."))))
      out))
  ([conf ^java.net.URI src-path col-headers col-handlers]
    (start-worker conf src-path col-headers col-handlers batch-size)))
