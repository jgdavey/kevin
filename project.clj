(defproject kevin "0.1.2"
  :description "It's like Kevin Bacon is right here!"
  :url "http://imdb.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.nrepl "0.2.6"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [commons-codec "1.9"]
                 [hiccup "1.0.5"]
                 [enlive "1.1.5"]
                 [environ "1.0.0"]
                 [lib-noir  "0.9.4" :exclusions  [compojure com.fasterxml.jackson.core/jackson-core ring org.clojure/tools.reader org.clojure/core.cache]]
                 [ring-server "0.3.1"]
                 [ring "1.3.1"]
                 [compojure "1.1.8"]]
  :plugins [[lein-ring "0.8.13"]
            [lein-beanstalk "0.2.7"]]
  :aws {:access-key ~(System/getenv "AWS_ACCESS_KEY_ID")
        :secret-key ~(System/getenv "AWS_SECRET_KEY") }
  :ring {:handler kevin.system/handler
         :init kevin.system/init
         :destroy kevin.system/destroy }
  :profiles {:production
             {:ring {:open-browser? false :stacktraces? false :auto-reload? false}
              :dependencies [[com.datomic/datomic-pro "0.9.5067" :exclusions [joda-time]]]}
             :dev {:source-paths ["dev"]
                   :dependencies [[com.datomic/datomic-free "0.9.5067" :exclusions [joda-time]]
                                  [org.clojure/tools.namespace "0.2.4"]
                                  [org.clojure/java.classpath "0.2.2"]
                                  [javax.servlet/servlet-api "2.5"]
                                  [ring-mock "0.1.5"]]}}
  :jvm-opts ["-Xmx4g" "-server"]
  )
