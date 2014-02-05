(ns user
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer (pprint)]
            [clojure.repl :refer :all]
            [clojure.zip :as zip]
            [clojure.tools.namespace.repl :refer (refresh refresh-all)]
            [datomic.api :as d :refer (db q)]
            [kevin.system :as sys]
            [kevin.core :refer :all]))

(defonce system nil)

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


(comment

  (reset)

  ;; queue-based Breadth-first search
  (let [d (-> system :db :conn db)
        bfs (fn [root children-fn]
            ((fn step [queue]
               (lazy-seq
                (when (seq queue)
                  (let [node (peek queue)
                        children (map (partial conj node) (children-fn (last node)))]
                    (cons node
                          (step (into (pop queue) children)))))))
             (conj clojure.lang.PersistentQueue/EMPTY root)))
        a (actor-name->eid d "Barth, Clayton")
        actor-name (partial eid->actor-name d)
        kevin (actor-name->eid d "Bacon, Kevin (I)")
        tree (bfs [a] (comp (partial immediate-connections d)))]
    (time (map actor-name (some (fn [n] (when (= kevin (last n)) n)) tree))))

  ;; zipper
  (let [d (-> system :db :conn db)
        bfs (fn [root children-fn]
            ((fn step [queue]
               (lazy-seq
                (when (seq queue)
                  (let [node (peek queue)
                        children (map (partial conj node) (children-fn (last node)))]
                    (cons node
                          (step (into (pop queue) children)))))))
             (conj clojure.lang.PersistentQueue/EMPTY root)))
        a (actor-name->eid d "Barth, Clayton")
        actor-name (partial eid->actor-name d)
        kevin (actor-name->eid d "Bacon, Kevin (I)")
        tree (zipper d a)]
    (time (map actor-name (some (fn [n] (when (= kevin (last n)) n)) tree))))


  ;; another queue-based search

  (let [d (-> system :db :conn db)
        clay (actor-name->eid d "Barth, Clayton")
        kevin (actor-name->eid d "Bacon, Kevin (I)")
        neighbor-fn (partial immediate-connections d)
        actor-name (partial eid->actor-name d)]
    (time (map actor-name ((searcher clay neighbor-fn) kevin))))

)
