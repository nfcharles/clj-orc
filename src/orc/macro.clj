(ns orc.macro
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async])
  (:gen-class))


(defn sort-key [^java.io.File f]
  (* -1 (count (.getPath f))))

(defn rm-files [root]
  ;; Sort order guarantees that all child elements are deleted
  ;; before parents.
  (doseq [f (sort-by sort-key (file-seq root))]
    (io/delete-file f)))

(defn mkdirp [path]
  (let [dir (io/file path)]
    (if (.exists dir)
      true
      (.mkdirs dir))))

(defn ws-name [prefix]
  (format ".tmp-ws-%s-%s-%s" prefix (rand-int Integer/MAX_VALUE) (System/currentTimeMillis)))

(defn mk-workspace [prefix]
  (let [ws (format "/tmp/%s" (ws-name prefix))]
    (mkdirp ws)
    ws))

(defmacro with-tmp-workspace [bindings & body]
  (assert (vector? bindings) "vector required for binding")
  (assert (== (count bindings) 2) "only one binding pair allowed")
  (assert (symbol? (bindings 0)) "only symbol allowed in binding")
  `(let [~(bindings 0) (mk-workspace ~(bindings 1))]
     (try
       ~@body
       (finally
         ;(println (format "Removing workspace %s" ~(bindings 0)))
         (rm-files (io/file ~(bindings 0)))))))

(defmacro with-async-record-reader [bindings & body]
  "(with-record-reader [rr (...)]
      (...)

    where
      * rr: record reader for iterating thru batches"
  (assert (vector? bindings) "vector required for binding")
  (assert (== (count bindings) 2) "one binding pairs expected")
  (assert (symbol? (bindings 0)) "only symbol allowed in binding")
  `(let ~(subvec bindings 0 2)
     (async/thread
       (try
         ~@body
         (finally
           (.close ~(bindings 0)))))))
