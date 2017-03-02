(defproject coinbase-api "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-cljfmt "0.5.6"]]
  :dependencies [[org.clojure/clojure "1.9.0-alpha12"]
                 [clj-http "2.0.0"]
                 [stylefruits/gniazdo "0.4.1"]
                 [org.clojure/data.json "0.2.6"]
                 [clj-time "0.11.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.apache.commons/commons-math3 "3.2"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "0.9.0"]]}})
