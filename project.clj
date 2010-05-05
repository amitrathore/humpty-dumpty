(defproject humpty-dumpty "1.0-SNAPSHOT"
  :description "Humpty Dumpty"
  :url "http://github.com/amitrathore/humpty-dumpty"
  :test-path "spec"
  :dependencies [[org.clojure/clojure "1.1.0"]
                 [org.clojure/clojure-contrib "1.1.0"]
                 [org.danlarkin/clojure-json "1.1-SNAPSHOT"]
                 [clojure-json "1.1-SNAPSHOT"]
                 [redis-clojure "1.0-SNAPSHOT"]]
  :dev-dependencies [[leiningen/lein-swank "1.2.0-SNAPSHOT"]
                     [swank-clojure "1.2.0-SNAPSHOT"]])