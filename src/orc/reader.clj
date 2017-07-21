(ns orc.reader
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
	    [clojure.core.async :as async]
	    [orc.macro :refer [with-async-record-reader]])
  (:import (org.apache.orc.OrcFile)
           (org.apache.orc.OrcFile.ReaderOptions)
           (org.apache.hadoop.fs.Path)
	   (org.apache.hadoop.conf.Configuration))
  (:gen-class))

(def buffer-size 10)

(def config-mapping
  (hash-map
    "fs.file.impl" "org.apache.hadoop.fs.LocalFileSystem"))

(defn orc-config [& ops]
  (let [conf (org.apache.hadoop.conf.Configuration.)]
    (println "CONFIGURATION")
    (doseq [[k v] config-mapping]
      (println (format "%s=%s" k v))
      (.set conf k v))
    conf))

(defn get-col [bat n]
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

(defn rows->map [col-config bat]
  (let [n-rows (.count bat)]
    (loop [row 0
           rcrds []]
      (if (< row n-rows)
        (let [rec (row->map col-config bat row)]
          (recur (inc row) (conj rcrds rec)))
        rcrds))))

(defn reader [path conf]
  (org.apache.orc.OrcFile/createReader path (org.apache.orc.OrcFile/readerOptions conf)))

(defn write [dest-path rows thrd-n]
  (if (> (count rows) 0)
    (with-open [wtr (io/writer dest-path)]
      (println (format "Writing %s" dest-path))
      (try
        (.write wtr (json/write-str rows))
        (println (format "Done writing %s" dest-path))
	(catch Exception e
	  (println e)))
      (println (format "count.writer.thread_%s=%s" thrd-n (count rows))))
    (println (format "Not writing %s; no records" dest-path))))

(defn start-writers [in-ch prefix n]
  (let [out-ch (async/chan)
        active-threads (atom n)
        filename #(format "%s-part-%d-%d.json" %1 %2 %3)] ; <pfx>-part-<thrd-no>-<grp-n>.json
    (dotimes [thrd-n n]
      (async/thread
        (println (format "Staring writer thread-%d" thrd-n))
        (loop [grp-n 0]
          (if-let [rows (async/<!! in-ch)]
            (let [file (filename prefix thrd-n grp-n)] 
              (write file rows thrd-n)
	      (recur (inc grp-n)))
	    (do
              (swap! active-threads dec)
              (when (= @active-threads 0)
                (async/>!! out-ch :close)
                (async/close! out-ch)))))))
    out-ch))

(defn start-worker [in-ch conf src-path col-headers col-handlers limit]
  (let [rdr (reader (org.apache.hadoop.fs.Path. src-path) conf)
        bat (.createRowBatch (.getSchema rdr))]
    (with-async-record-reader [rr (.rows rdr)]
      (println "Starting worker thread...")
      (loop [n 0
             acc []]
        ;; TODO: consider better ways of row accumulation / header init
        (if (.nextBatch rr bat)
	  (let [rows (rows->map (col-handlers bat) bat)
	        total (+ (count rows) n)]
	    (if (<= total limit)
              (recur total (concat acc rows))
	      (do
	        (async/>!! in-ch (concat [(col-headers bat)] acc rows))
	        (recur 0 []))))
          (do
	    (async/>!! in-ch acc)
	    (async/close! in-ch)))))))


(defn run [conf src-path dest-path-prefix col-headers col-handlers wrt-thread-count limit]
  (let [pipe (async/chan buffer-size)]
    (start-worker pipe conf src-path col-headers col-handlers limit)
    ;; Thread macro uses daemon threads so we must explicitly block
    ;; until all writer threads are complete to prevent premature
    ;; termination of main thread.
    (async/<!! (start-writers pipe dest-path-prefix wrt-thread-count))))

(defn orc->json [src-path dest-path-prefix col-headers col-handlers wrt-thread-count limit]
  (run (orc-config) src-path dest-path-prefix col-headers col-handlers wrt-thread-count limit))
