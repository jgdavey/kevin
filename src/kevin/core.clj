(ns kevin.core
  (:require [datomic.api :as d :refer [q db]]
            [clojure.set :refer [union]]))

(declare acted-with-rules)

(defn referring-to
  "Find all entities referring to an eid as a certain attribute."
  [db eid]
   (->> (d/datoms db :vaet eid)
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

(defn immediate-connections
  "d is database value
  eid is actor's entity id"
  [d eid]
  (->> (d/entity d eid)
      :movies
      (mapcat (comp (partial referring-to d) :db/id))
       set))

(defn append-immediate-connections [d id-path]
  (->> (immediate-connections d (last id-path))
       (map (partial conj id-path))))

(defn connections-at-degree
  "d is database value
  eid is actor's entity id
  deg is degree of separation (> 0)"
  [d deg eid]
  (let [e (d/entid d eid)]
    (if (zero? deg)
      #{e}
      (->> (immediate-connections d e)
           (pmap (partial connections-at-degree d (dec deg)))
           (apply union)))))

(def acted-with-rules
  '[[(acted-with-1 ?e ?x)
     [?e :movies ?m]
     [?x :movies ?m]
     [(!= ?e ?x)]]

    [(acted-with-2 ?e1 ?e2)
     (acted-with-1 ?e1 ?e2)]
    [(acted-with-2 ?e1 ?e2)
     (acted-with-1 ?e1 ?x)
     (acted-with-1 ?x ?e2)
     [(!= ?e1 ?e2)]]

    [(acted-with-3 ?e1 ?e2)
     (acted-with-2 ?e1 ?e2)]
    [(acted-with-3 ?e1 ?e2)
     (acted-with-2 ?e1 ?x)
     (acted-with-2 ?x ?e2)
     [(!= ?e1 ?e2)]]])
