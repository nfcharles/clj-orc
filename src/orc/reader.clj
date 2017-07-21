(ns orc.reader
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
	    [clojure.core.async :as async]
	    [orc.macro :refer [with-async-batches]])
  (:import (org.apache.orc.OrcFile)
           (org.apache.orc.OrcFile.ReaderOptions)
           (org.apache.hadoop.fs.Path)
	   (org.apache.hadoop.conf.Configuration))
  (:gen-class))


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
        (recur (inc col-n) (rest col-conf) (assoc! rcrd (conf :name) val)))
      (persistent! rcrd))))

(defn rows->map [col-config bat]
  (let [n-rows (.count bat)]
    (loop [row 0
           rcrds []]
      (if (< row n-rows)
        (let [rec (row->map col-config bat row)]
          (recur (inc row) (conj rcrds rec)))
        rcrds))))

(defn fs-path [path]
  (org.apache.hadoop.fs.Path. path))

(defn reader [path conf]
  (org.apache.orc.OrcFile/createReader path (org.apache.orc.OrcFile/readerOptions conf)))

(defn batch [rdr]
  (.createRowBatch (.getSchema rdr)))

;;;
;;; --- NEW ---
;;;

(defn write [dest-path rows thrd-n]
  (if (> (count rows) 0)
    (with-open [wtr (io/writer dest-path)]
      (println (format "Writing %s" dest-path))
      (try
        (.write wtr (json/write-str rows))
	(catch Exception e
	  (println e)))
      (println (format "count.writer.thread_%s=%s" thrd-n (count rows))))
    (println (format "Not writing %s; no records" dest-path))))

(defn- writer [in-ch prefix n]
  (let [out-ch (async/chan)
        active-threads (atom n)
        filename #(format "%s-part-%d-%d.json" %1 %2 %3)] ; <pfx>-part-<thrd-no>-<grp-n>.json
    (dotimes [thrd-n n]
      (async/thread
        (println (format "Staring writer thread-%d" thrd-n))
        (loop [grp-n 0]
          (if-let [rows (async/<!! in-ch)]
            (do
              (write (filename prefix thrd-n grp-n) rows thrd-n)
	      (recur (inc grp-n)))
	    (do
              (swap! active-threads dec)
              (when (= @active-threads 0)
                (async/>!! out-ch :close)
                (async/close! out-ch)))))))
    out-ch))


(defn process [in-ch conf src-path limit column-handlers]
  (with-async-batches [rdr (reader (fs-path src-path) conf)
                       bat (batch rdr)
		       ch in-ch
                       wrt-limit limit]
    (rows->map (column-handlers bat) bat)))

(defn run [conf src-path dest-path-prefix limit column-handlers]
  (let [pipe (async/chan)]
    (process pipe conf src-path limit column-handlers)
    ;; Thread macro uses daemon threads so we must explicitly block
    ;; until all writer threads are complete to prevent premature
    ;; termination of main thread.
    (async/<!! (writer pipe dest-path-prefix 2))))

(defn orc->json [src-path dest-path-prefix limit column-handlers]
  (run (orc-config) src-path dest-path-prefix limit column-handlers))
