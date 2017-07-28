(defproject orc "0.4.0-SNAPSHOT"
  :description "ORC -> json converter"
  :url "https://github.com/nfcharles/clj-orc.git"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/data.json "0.2.6"]
		 [org.clojure/core.async "0.2.374"]
		 [orc-rw-mini "1.4.0"]]
  :repositories {"local" "file:maven_repository"}
  :main orc.write
  :target-path "target/%s"
  :global-vars {*warn-on-reflection* true}
  ;:jvm-opts ["-Dcom.sun.management.jmxremote"
  ;           "-Dcom.sun.management.jmxremote.ssl=false"
  ;           "-Dcom.sun.management.jmxremote.authenticate=false"
  ;           "-Dcom.sun.management.jmxremote.port=43210"]
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[com.amazonaws/aws-java-sdk-s3 "1.11.163"]
                                  [com.amazonaws/aws-java-sdk-core "1.11.163"]]}})
