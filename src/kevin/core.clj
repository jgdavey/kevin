(ns kevin.core
  (:require [datomic.api :as d :refer [q db]]
            [clojure.string :refer [split join]]
            [clojure.set :refer [union difference]]
            [clojure.zip :as zip]))

(defprotocol Eid
  (e [_]))

(extend-protocol Eid
  java.lang.Long
  (e [i] i)

  datomic.Entity
  (e [ent] (:db/id ent)))

(def acted-with-rules
  '[[(acted-with ?e1 ?e2 ?path)
     [?e1 :movies ?m]
     [?e2 :movies ?m]
     [(!= ?e1 ?e2)]
     [(vector ?e1 ?m) ?path]]
    [(acted-with-2 ?e1 ?e2 ?path)
     (acted-with ?e1 ?e2 ?path)]
    [(acted-with-2 ?e1 ?e2 ?path)
     (acted-with ?e1 ?x ?p1)
     (acted-with ?x ?e2 ?p2)
     [(concat ?p1 ?p2) ?path]]
    [(acted-with-3 ?e1 ?e2 ?path)
     (acted-with-2 ?e1 ?e2 ?path)]
    [(acted-with-3 ?e1 ?e2 ?path)
     (acted-with-2 ?e1 ?x ?p1)
     (acted-with ?x ?e2 ?p2)
     [(concat ?p1 ?p2) ?path]]])

(defn format-query
  "Makes each word of query required, front-stemmed

  (format-query \"Foo bar\")
   ;=> \"+Foo* +bar*\"

  This maps to Lucene's QueryParser.parse
  See http://lucene.apache.org/core/3_6_1/api/core/org/apache/lucene/queryParser/QueryParser.html"
  [query]
  (->> (split query #"\s")
       (map #(str "+" % "*"))
       (join " ")))

(defn actor-search
  "query will be passed as-is to Lucene"
  [db query]
  (q '[:find ?e ?name
       :in $ ?search
       :where [(fulltext $ :actor/name ?search) [[?e ?name]]]]
     db query))

(defn actor-or-movie-name [db eid]
  (let [ent (d/entity db (e eid))]
    (or (:movie/title ent) (:actor/name ent))))

(defn referring-to
  "Find all entities referring to an eid as a certain attribute."
  [db eid]
   (->> (d/datoms db :vaet (e eid))
        (map :e)))

(defn eids-with-attr-val
  "Return eids with a given attribute and value."
  [db attr val]
  (->> (d/datoms db :avet attr val)
       (map :e)))

(defn actor-name->eid
  "db is database value
  name is the actor's name"
  [db name]
  (-> (eids-with-attr-val db :actor/name name)
    first))

(defn eid->actor-name
  "db is database value
  name is the actor's name"
  [db eid]
  (-> (d/entity db (e eid))
      :actor/name))

(defn actor-movies
  [db eid]
  (map :v (d/datoms db :eavt eid :movies)))

(defn immediate-connections
  "d is database value
  eid is actor's entity id"
  [db eid]
  (->> (actor-movies db eid)
      (mapcat (partial referring-to db))))

(defn zipper
  "db is database value
  eid is actor's entity id"
  [db eid]
  (let [children (partial immediate-connections db)
        branch? (comp seq children)
        make-node (fn [_ c] c)]
    (zip/zipper branch? children make-node eid)))

(defn searcher [root neighbor-fn]
  (fn [target]
    (let [queue (conj clojure.lang.PersistentQueue/EMPTY [root])
          visited #{root}
          target-neighbors (neighbor-fn target)
          found? (fn [n] (some #{n} target-neighbors))]
      (loop [q queue
             v visited
             i 0]
        (when (seq q)
          (let [path (peek q)
                node (last path)]
            (if (found? node)
              (do
                (println "Finished in " i " iterations")
                (conj path target))
              (let [neighbors (remove v (neighbor-fn node))
                    paths (map (partial conj path) neighbors)]
                (recur (into (pop q) paths) (into v neighbors) (inc i))))))))))
