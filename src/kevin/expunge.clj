(ns kevin.expunge
  (:require [datomic.api :as d :refer [q db]]
            [clojure.java.io :as io]
            [kevin.loader :refer :all]))

(defn parse-movie-titles [coll]
  (->> (doall coll)
       (filter movie-line?)
       (map movie-title)))

(defn movie-titles
  "This is documentation"
  ([path]
   (load-file-with-parser path parse-movie-titles :start-at "MOVIES LIST")))

(defn actor-names [path db]
  (let [titles (movie-titles path)]
    (q '[:find ?name ?title
         :in $ [?title ...]
         :where [?m :movie/title ?title]
         [?a :movies ?m]
         [?a :actor/name ?name]]
      db titles)))

