(ns kevin.expunge
  (:require [datomic.api :as d :refer [q db]]
            [clojure.java.io :as io])
  (:use [kevin.loader :exclude [load-file-with-parser]]))

(defn parse-movie-titles [coll]
  (->> coll
       (filter movie-line?)
       (map movie-title)))

(defn load-file-with-parser
  [file parser & {:keys [start-at]}]
    (let [in (io/reader file :encoding "ISO-8859-1")
          lines (line-seq in)]
      (loop [[line & lines] lines
            state { :header true }]
        (if (:header state)
          (recur lines { :header (not= line start-at) })
          (parser lines)))))

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

