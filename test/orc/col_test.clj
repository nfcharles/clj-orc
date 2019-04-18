(ns orc.col-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [orc.fixture :as orc-fixture]
            [orc.macro :refer [with-tmp-workspace]]
            [orc.col :as orc.col]))


(def fields (list
  {:name "foo" :type :string}
  {:name "bar" :type :int}))

(def fields-handlers (vector
  {:name "foo" :fn orc.col/parse-bytes}
  {:name "bar" :fn orc.col/parse-long}))

(deftest col-test
  (testing "Translate type list to  type handlers"
    #_(is (= (orc.col/handlers fields) fields-handlers))))
