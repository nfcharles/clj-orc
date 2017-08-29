(ns orc.read-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [orc.write :as orc-write]
            [orc.macro :refer [with-tmp-workspace]]
            [orc.read :as orc-read]
	    [orc.core :as orc-core])
  (import [java.net URI]))

(orc-core/configure-logging :std-err)

(deftest read-test
  (with-tmp-workspace [ws "test"]
    (let [file (orc-write/write (format "%s/foo.orc" ws) 10)
          ch (orc-read/start-worker (orc-read/configure)
                                    (URI. file)
                                    orc-write/column-headers
                                    orc-write/column-handlers
                                    4)]
      (testing "Read ORC -> clj repr"
        (is (= (async/<!! ch) {0 "f1", 1 "f2"}))
        (is (= (async/<!! ch) {:i 1, :rows [{0 0, 1 0} {0 1, 1 2} {0 2, 1 4} {0 3, 1 6}]}))
        (is (= (async/<!! ch) {:i 2, :rows [{0 4, 1 8} {0 5, 1 10} {0 6, 1 12} {0 7, 1 14}]}))
        (is (= (async/<!! ch) {:i 3, :rows [{0 8, 1 16} {0 9, 1 18}]}))
        (is (= (async/<!! ch) nil))))))
