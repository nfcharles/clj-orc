(ns orc.macro
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async])
  (:gen-class))


(defn rm-files [root]
  ;; Sort order guarantees that all child elements are deleted
  ;; before parents.
  (doseq [f (sort-by #(* -1 (count (.getPath %))) (file-seq root))]
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
  (assert (vector? bindings) "vector required for binding")
  (assert (== (count bindings) 6) "three binding pairs expected")
  (assert (symbol? (bindings 0)) "only symbol allowed in first binding")
  (assert (symbol? (bindings 2)) "only symbol allowed in second binding")
  (assert (symbol? (bindings 4)) "only symbol allowed in third binding")
  `(let ~(subvec bindings 0 6)
     (let [rr# (.rows ~(bindings 0))]
       (try
         (loop [res# []]
           (if (.nextBatch rr# ~(bindings 2))
             (recur (concat res# ~@body))
             (~(bindings 4) res#)))
         (finally
           (.close rr#))))))


(def buffer-size 10)

(defmacro with-async-batches [bindings & body]
  "(with-batches [rdr (...)
                  bat (...)
                  out-ch (...)
                  thread-mapping (...)]
      (process-batch ...))

    where
      * rdr: file reader
      * bat: batch abstraction
      * wrt: write func that accepts accumulated batch rows"
  (assert (vector? bindings) "vector required for binding")
  (assert (== (count bindings) 8) "three binding pairs expected")
  (assert (symbol? (bindings 0)) "only symbol allowed in first binding")
  (assert (symbol? (bindings 2)) "only symbol allowed in second binding")
  (assert (symbol? (bindings 4)) "only symbol allowed in third binding")
  (assert (symbol? (bindings 6)) "only symbol allowed in fourth binding")
  `(let ~(subvec bindings 0 8)
     (let [rr# (.rows ~(bindings 0))
           limit# ~(bindings 6)]
       (println (format "rows.limit=%d" limit#))
       (async/thread
         (println "Starting worker thread...")
         (try
           (loop [n# 0
	          acc# []]
             (if (.nextBatch rr# ~(bindings 2))
	       (let [rows# (do ~@body)
	             total# (+ (count rows#) n#)]
	         (if (<= total# limit#)
	           (recur total# (concat acc# rows#))
                   (do
                     (async/>!! ~(bindings 4) (concat acc# rows#))
                     (recur 0 []))))
               (do
                 (async/>!! ~(bindings 4) acc#)
		 (async/close! ~(bindings 4)))))
           (finally
             (.close rr#)))))))
