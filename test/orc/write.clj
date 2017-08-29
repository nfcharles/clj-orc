(ns orc.write
  (:require [orc.macro :refer [with-tmp-workspace]]
            [orc.col :as col])
  (import [org.apache.orc OrcFile]
          [org.apache.hadoop.fs Path]
          [org.apache.hadoop.conf Configuration]
	  [org.apache.orc Writer
	                  TypeDescription]
	  [org.apache.hadoop.hive.ql.exec.vector VectorizedRowBatch
                                                 ColumnVector
                                                 BytesColumnVector
                                                 LongColumnVector
                                                 DoubleColumnVector]
          [java.util Random
                     UUID])
  (:gen-class))


(defn configuration ^Configuration []
  (Configuration.))

(defn schema ^TypeDescription []
  (TypeDescription/fromString "struct<x:int,y:int>"))

(defn writer ^Writer [^Configuration conf ^Path path ^TypeDescription sch]
  (OrcFile/createWriter path (-> (OrcFile/writerOptions conf)
                                 (.setSchema sch))))

(defn batch
  (^VectorizedRowBatch [^TypeDescription sch size]
    (.createRowBatch sch size))
  (^VectorizedRowBatch [^TypeDescription sch ]
    (batch sch 5)))

(defn long-col ^LongColumnVector [^VectorizedRowBatch bat n]
  (aget (.cols bat) n))

(defn write-batch [^Writer wtr ^TypeDescription sch n]
  (let [bat (batch sch)
        size (.getMaxSize bat)
	#^longs v1 (.vector (long-col bat 0))
	#^longs v2 (.vector (long-col bat 1))]
    (loop [row 0]
      (if (< row n)
        (let [r (mod row size)]
          (aset v1 r row)
          (aset v2 r (* row 2))
	  (set! (.size bat) (inc r))

          (if (= (.size bat) (.getMaxSize bat))
	    (do
              (.addRowBatch wtr bat)
              (.reset bat)
	      (recur (inc row)))
	    (recur (inc row))))
        (when (> (.size bat) 0)
          (.addRowBatch wtr bat)
          (.reset bat))))))

(defn write [file n]
  (let [sch (schema)
        wtr (writer (configuration) (Path. file) sch)]
    (println (format ">>> Writing test file:%s <<<" file))
    (write-batch wtr sch n)
    (.close wtr)
    file))

(def fields
  (list
    {:name "f1" :type "int" }
    {:name "f2" :type "long" }))
    ;{:name "f3" :type "float" }
    ;{:name "f4" :type "double" }
    ;{:name "f5" :type "boolean" }
    ;{:name "f6" :type "string" }))

(defn column-handlers [^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (col/handlers fields))

(defn hdr-reducer [acc item]
  (assoc acc (item 0) ((item 1) :name)))

(defn column-headers [^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (reduce hdr-reducer {} (map vector (range (count fields)) fields)))

;;;
;;;  -- TEST ENTRYPOINT --
;;;

(defn -main
  [& args]
  (with-tmp-workspace [ws "foo"]
    (write (format "%s/foo.orc" ws) 10)))
