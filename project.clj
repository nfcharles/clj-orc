(defproject orc "0.8.1-dev"
  :description "ORC -> json converter"
  :url "https://github.com/nfcharles/clj-orc.git"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
		 [org.clojure/core.async "0.2.374"]
                 [org.apache.orc/orc-core "1.5.5"]
                 [org.apache.hadoop/hadoop-common "2.8.3"]
		 [com.taoensso/timbre "4.10.0"]]
  :main orc.json
  :repositories {"local" "file:maven_repository"}
  :target-path "target/%s"
  :global-vars {*warn-on-reflection* true}
  ;:jvm-opts ["-Dcom.sun.management.jmxremote"
  ;           "-Dcom.sun.management.jmxremote.ssl=false"
  ;           "-Dcom.sun.management.jmxremote.authenticate=false"
  ;           "-Dcom.sun.management.jmxremote.port=43210"]
  :profiles {:uberjar {:aot :all}
             :dev  {:dependencies [[org.apache.hadoop/hadoop-aws "2.8.3"]]}
             :repl {:dependencies [[org.apache.hadoop/hadoop-aws "2.8.3"]]}})
