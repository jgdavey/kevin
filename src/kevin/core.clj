(ns kevin.core
  (:require [kevin.loader :refer [conn uri]]
            [datomic.api :as d :refer [q db]]
            [clojure.set :refer [union]]))

(defn actor-name->eid
  "d is database value
  name is the actor's name"
  [d name]
  (ffirst (q '[:find ?e
               :in $ ?name
               :where [?e :actor/name ?name]]
             d name)))

(defn immediate-connections
  "d is database value
  eid is actor's entity id"
  [d eid]
  (let [e (d/entid d eid)]
    (->> (q '[:find ?c
              :in $ ?eid
              :where
              [?eid :movies ?m]
              [?c :movies ?m]] d e)
         (map first)
         set)))

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
