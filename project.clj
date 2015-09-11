(defproject kevin "0.1.2"
  :description "It's like Kevin Bacon is right here!"
  :url "http://imdb.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.nrepl "0.2.10"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [commons-codec "1.10"]
                 [hiccup "1.0.5"]
                 [enlive "1.1.6"]
                 [environ "1.0.0"]
                 [lib-noir  "0.9.9" :exclusions  [compojure clout com.fasterxml.jackson.core/jackson-core ring org.clojure/tools.reader org.clojure/core.cache]]
                 [ring-server "0.4.0"]
                 [ring "1.4.0"]
                 [clj-time  "0.11.0"]
                 [compojure "1.4.0"]]
  :plugins [[lein-ring "0.9.6" :exclusions [org.clojure/clojure]]
            [lein-beanstalk "0.2.7" :exclusions [commons-codec org.clojure/clojure]]]
  :ring {:handler kevin.system/handler
         :init kevin.system/init
         :destroy kevin.system/destroy }
  :profiles {:uberjar
             {:ring {:open-browser? false :stacktraces? false :auto-reload? false}
              :dependencies [[com.datomic/datomic-pro "0.9.5206" :exclusions [joda-time]]]}
             :dev {:source-paths ["dev" "src"]
                   :dependencies [[com.datomic/datomic-free "0.9.5206" :exclusions [joda-time]]
                                  [org.clojure/tools.namespace "0.2.11"]
                                  [org.clojure/java.classpath "0.2.2"]
                                  [javax.servlet/servlet-api "2.5"]
                                  [ring-mock "0.1.5"]]}}
  :jvm-opts ["-Xmx4g" "-server"]
  )
