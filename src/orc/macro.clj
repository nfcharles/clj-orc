(ns orc.macro
  (:require [clojure.java.io :as io])
  (:gen-class))


(defn rm-files [root]
  (doseq [f (sort-by #(* -1 (count (.getPath %))) (file-seq root))]
    (io/delete-file f)))

(defn mkdirp [path]
  (let [dir (java.io.File. path)]
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
  (assert vector? bindings) "vector required for binding"
  (assert (== (count bindings) 2) "only one binding pair allowed")
  (assert (symbol? (bindings 0)) "only symbol allowed in binding")
  `(let [~(bindings 0) (mk-workspace ~(bindings 1))]
     (try
       ~@body
       (finally
         (println (format "Removing workspace %s" ~(bindings 0)))
         (rm-files (io/file ~(bindings 0)))))))


;; TODO: add appropriate assertions and error handling
(defmacro with-batches [bindings & body]
  "(with-batches [rdr (...)
                  bat (...)
                  wrt (...)]
      (process-batch ...))

    where
      * rdr: file reader
      * bat: batch abstraction
      * wrt: write func that accepts accumulated batch rows"
  `(let ~(subvec bindings 0 6)
     (let [rr# (.rows ~(bindings 0))]
       (try
         (loop [res# []]
           (if (.nextBatch rr# ~(bindings 2))
             (recur (concat res# ~@body))
             (~(bindings 4) res#)))
         (finally
           (.close rr#))))))
