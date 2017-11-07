(ns orc.json-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
	    [orc.read :as orc-read]
            [orc.fixture :as orc-fixture]
            [orc.macro :refer [with-tmp-workspace]]
            [orc.json :as orc-json]
	    [orc.core :as orc-core])
  (import [java.net URI]
          [org.apache.hadoop.fs Path]
          [org.apache.orc Writer TypeDescription]
          [org.apache.hadoop.hive.ql.exec.vector VectorizedRowBatch]))


(orc-core/configure-logging :std-err :info ["orc.*"])

(deftest test-json->map
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
          ch (orc-json/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :map)
                             (partial orc-fixture/column-handlers fields)
                             25 :bat-size 2 :buf-size 10 :coll-type :map)]
      (testing "translate ORC to json repr: multiple chunks"
        (is (= (async/<!! ch) "JSON Stream"))
        (is (= (async/<!! ch) {:i 1, :chunk "[{\"0\":\"f1\",\"1\":\"f2\"},{\"0\":0,\"1\":0},{\"0\":1,\"1\":2},{\"0\":2,\"1\":4},{\"0\":3,\"1\":6}"}))
        (is (= (async/<!! ch) {:i 2, :chunk ",{\"0\":4,\"1\":8},{\"0\":5,\"1\":10}"}))
        (is (= (async/<!! ch) {:i 3, :chunk ",{\"0\":6,\"1\":12},{\"0\":7,\"1\":14}"}))
        (is (= (async/<!! ch) {:i 4, :chunk ",{\"0\":8,\"1\":16},{\"0\":9,\"1\":18}"}))
        (is (= (async/<!! ch) {:i 5, :chunk "]"})))))
  (with-tmp-workspace [ws "test-2"]
    (let [src (str ws "/test.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
          ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 2
	      [(.vector (aget (.cols bat) 0)) identity]
	      [(.vector (aget (.cols bat) 1)) #(* % 2)])
          ch (orc-json/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :map)
                             (partial orc-fixture/column-handlers fields)
                             25 :bat-size 2 :buf-size 10 :coll-type :map)]
      (testing "translate ORC to json repr: one chunk"
        (is (= (async/<!! ch) "JSON Stream"))
        (is (= (async/<!! ch) {:i 1, :chunk "[{\"0\":\"f1\",\"1\":\"f2\"},{\"0\":0,\"1\":0},{\"0\":1,\"1\":2}]"}))
        (is (= (async/<!! ch) nil)))))
  (with-tmp-workspace [ws "test-3"]
    (let [src (str ws "/test.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
          ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          meta #(format "%d Columns" (.numCols %2))
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 2
	      [(.vector (aget (.cols bat) 0)) identity]
	      [(.vector (aget (.cols bat) 1)) #(* % 2)])
          ch (orc-json/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :map)
                             (partial orc-fixture/column-handlers fields)
                             25 :bat-size 2 :buf-size 10 :coll-type :map :meta meta)]
      (testing "translate ORC to json repr: custom meta function"
        (is (= (async/<!! ch) "2 Columns"))
        (is (= (async/<!! ch) {:i 1, :chunk "[{\"0\":\"f1\",\"1\":\"f2\"},{\"0\":0,\"1\":0},{\"0\":1,\"1\":2}]"}))
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
          ch (orc-json/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :map)
                             (partial orc-fixture/column-handlers fields)
                             25 :bat-size 2 :buf-size 10 :coll-type :map)]
      (testing "translate ORC to json repr: empty json"
        (is (= (async/<!! ch) "JSON Stream"))
        (is (= (async/<!! ch) {:i 1, :chunk "[{\"0\":\"f1\",\"1\":\"f2\"}]"}))
        (is (= (async/<!! ch) nil))))))


(deftest test-json->vector
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
          ch (orc-json/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :vector)
                             (partial orc-fixture/column-handlers fields)
                             25 :bat-size 2 :buf-size 10 :coll-type :vector)]
      (testing "translate ORC to json repr: multiple chunks"
        (is (= (async/<!! ch) "JSON Stream"))
        (is (= (async/<!! ch) {:i 1, :chunk "[[\"f1\",\"f2\"],[0,0],[1,2],[2,4],[3,6]"}))
        (is (= (async/<!! ch) {:i 2, :chunk ",[4,8],[5,10],[6,12],[7,14]"}))
        (is (= (async/<!! ch) {:i 3, :chunk ",[8,16],[9,18]]"}))
        (is (= (async/<!! ch) nil)))))
  (with-tmp-workspace [ws "test-2"]
    (let [src (str ws "/test.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
          ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 2
	      [(.vector (aget (.cols bat) 0)) identity]
	      [(.vector (aget (.cols bat) 1)) #(* % 2)])
          ch (orc-json/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :vector)
                             (partial orc-fixture/column-handlers fields)
                             25 :bat-size 2 :buf-size 10 :coll-type :vector)]
      (testing "translate ORC to json repr: one chunk"
        (is (= (async/<!! ch) "JSON Stream"))
        (is (= (async/<!! ch) {:i 1, :chunk "[[\"f1\",\"f2\"],[0,0],[1,2]]"}))
        (is (= (async/<!! ch) nil)))))
  (with-tmp-workspace [ws "test-3"]
    (let [src (str ws "/test.orc")
          ^TypeDescription sch (orc-fixture/schema "struct<x:int,y:int>")
          ^Writer wtr (orc-fixture/writer (orc-fixture/configuration) (Path. src) sch)
          ^VectorizedRowBatch bat (orc-fixture/batch sch 5)
          meta #(format "%d Columns" (.numCols %2))
          fields (list
            {:name "f1" :type "int"}
            {:name "f2" :type "int"})
          _ (orc-fixture/write wtr bat 2
	      [(.vector (aget (.cols bat) 0)) identity]
	      [(.vector (aget (.cols bat) 1)) #(* % 2)])
          ch (orc-json/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :vector)
                             (partial orc-fixture/column-handlers fields)
                             25 :bat-size 2 :buf-size 10 :coll-type :vector :meta meta)]
      (testing "translate ORC to json repr: custom meta function"
        (is (= (async/<!! ch) "2 Columns"))
        (is (= (async/<!! ch) {:i 1, :chunk "[[\"f1\",\"f2\"],[0,0],[1,2]]"}))
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
          ch (orc-json/start (orc-read/configure)
                             (URI. src)
                             (partial orc-fixture/column-headers fields :vector)
                             (partial orc-fixture/column-handlers fields)
                             25 :bat-size 2 :buf-size 10 :coll-type :vector)]
      (testing "translate ORC to json repr: empty json"
        (is (= (async/<!! ch) "JSON Stream"))
        (is (= (async/<!! ch) {:i 1, :chunk "[[\"f1\",\"f2\"]]"}))
        (is (= (async/<!! ch) nil))))))
