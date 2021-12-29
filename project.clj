(defproject org.clojars.nfcharles/orc "0.8.2"
  :description "ORC reader"
  :url "https://github.com/nfcharles/clj-orc.git"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/core.async "0.2.374"]
                 [org.apache.orc/orc-core "1.5.5"]
                 [joda-time/joda-time "2.9.4"]
                 [com.taoensso/timbre "4.10.0"]]
  :repositories {"local" "file:maven_repository"}
  :target-path "target/%s"
  :global-vars {*warn-on-reflection* true}
  :profiles {:uberjar {:aot :all}
             :dev  {:dependencies [[org.apache.hadoop/hadoop-aws "2.8.3"]
                                   [org.apache.hadoop/hadoop-common "2.8.3"]]}}
  :deploy-repositories {"releases" {:url "https://repo.clojars.org" :creds :gpg}})
