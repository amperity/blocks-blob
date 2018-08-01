(defproject amperity/blocks-blob "0.0.1"
  :description "Content-addressable Azure Blob block store."
  :url "https://github.com/amperity/blocks-blob"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :aliases
  {"coverage" ["with-profile" "+coverage" "cloverage"
               "--ns-exclude-regex" "blocks.store.tests"]}

  :deploy-branches ["master"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.8.0"]
   [org.clojure/tools.logging "0.4.0"]
   [com.microsoft.azure/azure-storage "7.0.0"]
   [com.stuartsierra/component "0.3.2"]
   [mvxcvi/blocks "1.0.0"]
   [mvxcvi/multihash "2.0.2"]]

  :test-selectors
  {:default (complement :integration)
   :integration :integration}

  :profiles
  {:dev
   {:dependencies
    [[org.slf4j/slf4j-simple "1.7.21" :exclusions [org.slf4j/slf4j-api]]
     [mvxcvi/test.carly "0.4.1"]]}

   :repl
   {:source-paths ["dev"]
    :dependencies [[org.clojure/tools.namespace "0.2.11"]]
    :jvm-opts ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"]}

   :test
   {:jvm-opts ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"]}

   :coverage
   {:plugins [[lein-cloverage "1.0.10"]]
    :dependencies [[commons-logging "1.2"]
                   [riddley "0.1.14"]]
    :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
               "-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}})
