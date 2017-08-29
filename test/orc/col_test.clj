(ns orc.col-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [orc.write :as orc-write]
            [orc.macro :refer [with-tmp-workspace]]
            [orc.col :as orc-col]))

(def fields (list
  {:name "foo" :type "string"}
  {:name "bar" :type "int"}))

(def fields-handlers (vector
  {:name "foo" :fn orc-col/string}
  {:name "bar" :fn orc-col/intg}))

(deftest col-test
  (testing "Type list -> type handlers"
    (is (= (orc-col/handlers fields) fields-handlers))))
