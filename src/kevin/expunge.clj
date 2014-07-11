(ns kevin.expunge
  (:require [datomic.api :as d :refer [q db]]
            [clojure.java.io :as io]
            [kevin.loader :refer [ensure-transformed-movies]]))

(defn movie-titles
  "This is documentation"
  ([path]
   (let [cleaned-path (clojure.string/replace path ".gz" "")
         cleaned-path (clojure.string/replace cleaned-path ".list" ".transformed")]
     (ensure-transformed-movies path cleaned-path)
     (doall (line-seq (io/reader cleaned-path))))))

(defn actor-names [path db]
  (let [titles (movie-titles path)]
    (q '[:find ?name ?title
         :in $ [?title ...]
         :where [?m :movie/title ?title]
         [?a :actor/movies ?m]
         [?a :person/name ?name]]
      db titles)))
