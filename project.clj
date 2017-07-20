(defproject clj-orc "0.1.0-SNAPSHOT"
  :description "ORC -> json converter"
  :url "https://github.com/nfcharles/clj-orc.git"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/data.json "0.2.6"]
		 [orc-rw-mini "1.4.0"]]
  ;:resource-paths ["resources/orc-rw-mini-1.4.0.jar"]
  :repositories {"local" "file:maven_repository"}
  :main orc.reader
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
