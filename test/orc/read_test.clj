(ns orc.read-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [orc.fixture :as orc-fixture]
            [orc.macro :refer [with-tmp-workspace]]
            [orc.read :as orc-read]
	    [orc.core :as orc-core])
  (import [java.net URI]
          [org.apache.hadoop.fs Path]
	  [org.apache.orc Writer TypeDescription]
	  [org.apache.hadoop.hive.ql.exec.vector VectorizedRowBatch]))


(orc-core/configure-logging :std-err)

(deftest simple-read-test
  (with-tmp-workspace [ws "test"]
    (let [src (str ws "/foo.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
	  ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 10
	      [(.vector (aget (.cols bat) 0)) identity]
	      [(.vector (aget (.cols bat) 1)) #(* % 2)])
          ch (orc-read/start-worker (orc-read/configure)
                                    (URI. src)
                                    (partial orc-fixture/column-headers fields)
                                    (partial orc-fixture/column-handlers fields)
                                    4)]
      (testing "translate ORC to native clj repr: 1"
        (is (= (async/<!! ch) {0 "f1", 1 "f2"}))
        (is (= (async/<!! ch) {:i 1, :rows [{0 0, 1 0} {0 1, 1 2} {0 2, 1 4} {0 3, 1 6}]}))
        (is (= (async/<!! ch) {:i 2, :rows [{0 4, 1 8} {0 5, 1 10} {0 6, 1 12} {0 7, 1 14}]}))
        (is (= (async/<!! ch) {:i 3, :rows [{0 8, 1 16} {0 9, 1 18}]}))
        (is (= (async/<!! ch) nil))))))
