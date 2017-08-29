(ns orc.fixture
  (:require [orc.macro :refer [with-tmp-workspace]]
            [orc.col :as col])
  (import [org.apache.orc OrcFile]
          [org.apache.hadoop.fs Path]
          [org.apache.hadoop.conf Configuration]
          [org.apache.orc Writer TypeDescription]
          [org.apache.hadoop.hive.ql.exec.vector VectorizedRowBatch
                                                 ColumnVector
                                                 BytesColumnVector
                                                 LongColumnVector
                                                 DoubleColumnVector])
  (:gen-class))


(defn configuration ^Configuration []
  (Configuration.))

(defn schema ^TypeDescription
  ([s]
    (TypeDescription/fromString s))
  ([]
    (TypeDescription/fromString "struct<x:int,y:int>")))

(defn writer ^Writer [^Configuration conf ^Path path ^TypeDescription sch]
  (OrcFile/createWriter path (-> (OrcFile/writerOptions conf)
                                 (.setSchema sch))))

(defn batch
  (^VectorizedRowBatch [^TypeDescription sch size]
    (.createRowBatch sch size))
  (^VectorizedRowBatch [^TypeDescription sch ]
    (batch sch 8)))

(defn long-col ^LongColumnVector [^VectorizedRowBatch bat n]
  (aget (.cols bat) n))

(defn write [^Writer wtr ^TypeDescription bat row-limit & vfns]
  (let [size (.getMaxSize bat)]
    (loop [row 0]
      (if (< row row-limit)
        (let [r (mod row size)]
	  (dotimes [i (count vfns)]
	    (let [[v fn] (nth vfns i)]
	      ;(println (format "%s=%s" v (fn row)))
	      (aset v r (fn row))))
	  (set! (.size bat) (inc r))

          (if (= (.size bat) (.getMaxSize bat))
	    (do
              (.addRowBatch wtr bat)
              (.reset bat)
	      (recur (inc row)))
	    (recur (inc row))))
        (when (> (.size bat) 0)
          (.addRowBatch wtr bat)
          (.reset bat))))
    (.close wtr)))

(defn column-handlers [flds ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (col/handlers flds))

(defn hdr-reducer [acc item]
  (assoc acc (item 0) ((item 1) :name)))

(defn column-headers [flds ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (reduce hdr-reducer {} (map vector (range (count flds)) flds)))
