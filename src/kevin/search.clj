(ns kevin.search
  (:require [datomic.api :as d :refer [q]]
            [kevin.core :refer :all]))

(defn name-or-search [db person]
  (let [actor (actor-name->eid db person)
        result {:name person :actor-id actor}]
    (assoc result :names
           (cond
             (not person) (list)
             actor (list person)
             :else (->> person
                        format-query
                        (actor-search db)
                        (map last))))))

(defn search [db & people]
  (mapv (partial name-or-search db) people))

(defn path-at-depth [db source target depth]
  (let [rule (symbol (str "acted-with-" depth))]
    (q (concat '[:find ?path
                 :in $ % ?actor ?target
                 :where]
               [(list rule '?actor '?target '?path)])
      db acted-with-rules source target)))

(defn find-paths [db source target]
  (let [depth (partial path-at-depth db source target)
        ename (partial actor-or-movie-name db)]
    (map (fn [[p]] (mapv ename (conj (vec p) target)))
         (or (seq (depth 1))
             (seq (depth 2))
             (seq (depth 3))))))
