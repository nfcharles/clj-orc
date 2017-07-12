(defproject clj-orc "0.1.0-SNAPSHOT"
  :description "ORC file converter -> json"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/data.json "0.2.6"]]
  :resource-paths ["resources/orc-tools-1.4.0-uber.jar"]
  :main orc.reader
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
