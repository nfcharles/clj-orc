(ns orc.json-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
	    [orc.read :as orc-read]
            [orc.write :as orc-write]
            [orc.macro :refer [with-tmp-workspace]]
            [orc.json :as orc-json]
	    [orc.core :as orc-core])
  (import [java.net URI]))

(orc-core/configure-logging :std-err)

(deftest json-test
  (with-tmp-workspace [ws "test"]
    (let [file (orc-write/write (format "%s/foo.orc" ws) 10)
          ch (orc-json/start-streamer (orc-read/configure)
                                    (URI. file)
                                    orc-write/column-headers
                                    orc-write/column-handlers
				    25
                                    2)]
      (testing "Read ORC -> json repr"
        (is (= (async/<!! ch) "JSON Stream"))
        (is (= (async/<!! ch) {:i 1, :chunk "[{\"0\":\"f1\",\"1\":\"f2\"},{\"0\":0,\"1\":0},{\"0\":1,\"1\":2},{\"0\":2,\"1\":4},{\"0\":3,\"1\":6}"}))
        (is (= (async/<!! ch) {:i 2, :chunk ",{\"0\":4,\"1\":8},{\"0\":5,\"1\":10}"}))
        (is (= (async/<!! ch) {:i 3, :chunk ",{\"0\":6,\"1\":12},{\"0\":7,\"1\":14}"}))
        (is (= (async/<!! ch) {:i 4, :chunk ",{\"0\":8,\"1\":16},{\"0\":9,\"1\":18}"}))
        (is (= (async/<!! ch) nil))))))
