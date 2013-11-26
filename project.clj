(defproject kevin "0.1.0-SNAPSHOT"
  :description "It's like Kevin Bacon is right here!"
  :url "http://imdb.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.datomic/datomic-pro "0.8.4260"]]
  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[org.clojure/tools.namespace "0.2.4"]
                                  [org.clojure/java.classpath "0.2.0"]]}}
  :jvm-opts ["-Xmx2g"]
  )
