(defproject kevin "0.1.0-SNAPSHOT"
  :description "It's like Kevin Bacon is right here!"
  :url "http://imdb.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.datomic/datomic-pro "0.9.4556"]
                 [hiccup "1.0.5"]
                 [ring-server "0.3.1"]
                 [compojure "1.1.6"]]
  :plugins [[lein-ring "0.8.10"]]
  :ring {:handler kevin.system/handler
         :init kevin.system/init
         :destroy kevin.system/destroy }
  :profiles {:production
             {:ring
              {:open-browser? false :stacktraces? false :auto-reload? false}}
             :dev {:source-paths ["dev"]
                   :dependencies [[org.clojure/tools.namespace "0.2.4"]
                                  [org.clojure/java.classpath "0.2.2"]
                                  [javax.servlet/servlet-api "2.5"]
                                  [ring-mock "0.1.5"]]}}
  :jvm-opts ["-Xmx1g" "-server"]
  )
