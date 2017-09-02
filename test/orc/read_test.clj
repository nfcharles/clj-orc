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

(deftest test-read->map
  (with-tmp-workspace [ws "test-1"]
    (let [src (str ws "/test.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
	  ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 10
	      [(.vector (aget (.cols bat) 0)) identity]
	      [(.vector (aget (.cols bat) 1)) #(* % 2)])
          ch (orc-read/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :map)
                             (partial orc-fixture/column-handlers fields)
                             4 :map)]
      (testing "translate ORC to native clj repr: batch size 4"
        (is (= (async/<!! ch) {0 "f1", 1 "f2"}))
        (is (= (async/<!! ch) {:i 1, :rows [{0 0, 1 0} {0 1, 1 2} {0 2, 1 4} {0 3, 1 6}]}))
        (is (= (async/<!! ch) {:i 2, :rows [{0 4, 1 8} {0 5, 1 10} {0 6, 1 12} {0 7, 1 14}]}))
        (is (= (async/<!! ch) {:i 3, :rows [{0 8, 1 16} {0 9, 1 18}]}))
        (is (= (async/<!! ch) nil)))))
  (with-tmp-workspace [ws "test-2"]
    (let [src (str ws "/test.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
	  ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 10
	      [(.vector (aget (.cols bat) 0)) identity]
	      [(.vector (aget (.cols bat) 1)) #(* % 2)])
          ch (orc-read/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :map)
                             (partial orc-fixture/column-handlers fields)
                             5 :map)]
      (testing "translate ORC to native clj repr: batch size 5"
        (is (= (async/<!! ch) {0 "f1", 1 "f2"}))
        (is (= (async/<!! ch) {:i 1, :rows [{0 0, 1 0} {0 1, 1 2} {0 2, 1 4} {0 3, 1 6} {0 4, 1 8}]}))
        (is (= (async/<!! ch) {:i 2, :rows [{0 5, 1 10} {0 6, 1 12} {0 7, 1 14} {0 8, 1 16} {0 9, 1 18}]}))
        (is (= (async/<!! ch) nil)))))
  (with-tmp-workspace [ws "test-3"]
    (let [src (str ws "/test.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
	  ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 5
	      [(.vector (aget (.cols bat) 0)) identity]
	      [(.vector (aget (.cols bat) 1)) #(* % 2)])
          ch (orc-read/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :map)
                             (partial orc-fixture/column-handlers fields)
                             5 :map)]
      (testing "translate ORC to native clj repr: 1 batch"
        (is (= (async/<!! ch) {0 "f1", 1 "f2"}))
        (is (= (async/<!! ch) {:i 1, :rows [{0 0, 1 0} {0 1, 1 2} {0 2, 1 4} {0 3, 1 6} {0 4, 1 8}]}))
        (is (= (async/<!! ch) nil)))))
  (with-tmp-workspace [ws "test-4"]
    (let [src (str ws "/test.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
	  ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 0)
          ch (orc-read/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :map)
                             (partial orc-fixture/column-handlers fields)
                             5 :map)]
      (testing "translate ORC to native clj repr: empty batch"
        (is (= (async/<!! ch) {0 "f1", 1 "f2"}))
        (is (= (async/<!! ch) {:i 1, :rows []}))
        (is (= (async/<!! ch) nil))))))


(deftest test-read->vector
  (with-tmp-workspace [ws "test-1"]
    (let [src (str ws "/test.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
	  ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 10
	      [(.vector (aget (.cols bat) 0)) identity]
	      [(.vector (aget (.cols bat) 1)) #(* % 2)])
          ch (orc-read/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :vector)
                             (partial orc-fixture/column-handlers fields)
                             4)]
      (testing "translate ORC to native clj repr: batch size 4"
        (is (= (async/<!! ch) ["f1" "f2"]))
        (is (= (async/<!! ch) {:i 1, :rows [[0 0] [1 2] [2 4] [3 6]]}))
        (is (= (async/<!! ch) {:i 2, :rows [[4 8] [5 10] [6 12] [7 14]]}))
        (is (= (async/<!! ch) {:i 3, :rows [[8 16] [9 18]]}))
        (is (= (async/<!! ch) nil)))))
  (with-tmp-workspace [ws "test-2"]
    (let [src (str ws "/test.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
	  ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 10
	      [(.vector (aget (.cols bat) 0)) identity]
	      [(.vector (aget (.cols bat) 1)) #(* % 2)])
          ch (orc-read/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :vector)
                             (partial orc-fixture/column-handlers fields)
                             5)]
      (testing "translate ORC to native clj repr: batch size 5"
        (is (= (async/<!! ch) ["f1" "f2"]))
        (is (= (async/<!! ch) {:i 1, :rows [[0 0] [1 2] [2 4] [3 6] [4 8]]}))
        (is (= (async/<!! ch) {:i 2, :rows [[5 10] [6 12] [7 14] [8 16] [9 18]]}))
        (is (= (async/<!! ch) nil)))))
  (with-tmp-workspace [ws "test-3"]
    (let [src (str ws "/test.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
	  ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 5
	      [(.vector (aget (.cols bat) 0)) identity]
	      [(.vector (aget (.cols bat) 1)) #(* % 2)])
          ch (orc-read/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :vector)
                             (partial orc-fixture/column-handlers fields)
                             5)]
      (testing "translate ORC to native clj repr: 1 batch"
        (is (= (async/<!! ch) ["f1" "f2"]))
        (is (= (async/<!! ch) {:i 1, :rows [[0 0] [1 2] [2 4] [3 6] [4 8]]}))
        (is (= (async/<!! ch) nil)))))
  (with-tmp-workspace [ws "test-4"]
    (let [src (str ws "/test.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
	  ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 0)
          ch (orc-read/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :vector)
                             (partial orc-fixture/column-handlers fields)
                             5)]
      (testing "translate ORC to native clj repr: empty batch"
        (is (= (async/<!! ch) ["f1" "f2"]))
        (is (= (async/<!! ch) {:i 1, :rows []}))
        (is (= (async/<!! ch) nil))))))
