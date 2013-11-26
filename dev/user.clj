(ns user
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer (pprint)]
            [clojure.repl :refer :all]
            [clojure.tools.namespace.repl :refer (refresh refresh-all)]
            [datomic.api :as d]
            [kevin.system :as sys]
            [kevin.core :refer :all]))

(def system nil)

(defn init
  "Constructs the current development system."
  []
  (alter-var-root #'system (constantly (sys/system))))

(defn start
  "Starts the current development system."
  []
  (alter-var-root #'system sys/start))

(defn stop
  "Shuts down and destroys the current development system."
  []
  (alter-var-root #'system (fn [s] (when s (sys/stop s)))))

(defn go
  "Initializes the current development system and starts it running."
  []
  (init)
  (start))

(defn reset []
  (stop)
  (refresh :after 'user/go))
