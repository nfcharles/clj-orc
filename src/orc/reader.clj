(ns orc.reader
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
	    [orc.macro :refer [with-batches]])
  (:import (org.apache.orc.OrcFile)
           (org.apache.orc.OrcFile.ReaderOptions)
           (org.apache.hadoop.fs.Path)
	   (org.apache.hadoop.conf.Configuration))
  (:gen-class))


(defn orc-config [& ops]
  (org.apache.hadoop.conf.Configuration.))

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

(defn write [wtr rows]
  (println (format "rows.count=%d" (count rows)))
  (.write wtr (json/write-str rows)))

(defn process [src-path dest-wtr conf column-handlers]
    (with-batches [rdr (reader (fs-path src-path) conf)
                   bat (batch rdr)
                   wrt (partial write dest-wtr)]
      (rows->map (column-handlers bat) bat)))

(defn orc->json [src-path dest-path column-handlers]
  (let [conf (orc-config)]
    (with-open [dest-wtr (io/writer dest-path)]	
      (process src-path dest-wtr conf column-handlers))))
