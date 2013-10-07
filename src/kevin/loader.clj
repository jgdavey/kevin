(ns kevin.loader
  "To use, download the movies, actors and actresses lists from a mirror on
  http://www.imdb.com/interfaces, and copy them (still zipped) to the resources
  folder. You can then run `lein run -m kevin.loader`"
  (:require [clojure.java.io                         :as io]
            [cheshire.custom                         :as json]
            [clj-http.client                         :as http]
            [clojurewerkz.neocons.rest               :as nr]
            [clojurewerkz.neocons.rest.cypher        :as cypher]
            [clojurewerkz.neocons.rest.nodes         :as nn]
            [clojurewerkz.neocons.rest.relationships :as nrel]
            [clojurewerkz.neocons.rest.batch :refer [perform]]
            [clojurewerkz.neocons.rest.records :refer [node-index-lookup-location-for]]))

(def url "http://localhost:7474/db/data/")
(def ^:dynamic *batch-size* 500)

(def char-quote "\"")
(def char-tab "\t")

(defn id->ref [i]
  (str "{" i "}"))

(defn create-batch-and-index [xs index attribute]
  (let [idx (str "/index/node/" index)
        batched (doall (mapcat (fn [x i]
                                 [{:body   x
                                   :id     i
                                   :to     "/node"
                                   :method "POST" }
                                 {:body   {
                                           :uri (id->ref i)
                                           :key attribute
                                           :value (attribute x) }
                                   :to     idx
                                   :method "POST" }]) xs (range)))]
    (perform batched)))

(defn movie-from-role [role]
  (try (nn/find-one "Movie" "title" role)))

(defn role-to-op [actor-uri role i]
  (if-let [movie (movie-from-role role)]
    {:body   {:to (:location-uri movie) :type "acted_in" }
     :to     (str actor-uri "/relationships")
     :method "POST" }))

(defn actor-to-op [actor-movies i]
  (let [{:keys [actor movies]} actor-movies
        idx "/index/node/Actor"
        ret-uri (id->ref i)
        actor-ops [{:body   { :name actor }
                    :id     i
                    :to     "/node"
                    :method "POST" }
                   {:body   {:uri ret-uri
                             :key :name
                             :value actor }
                    :to     idx
                    :method "POST" } ]]
    (concat actor-ops
            (filter identity (map (partial role-to-op ret-uri) movies (iterate inc (inc i)))))))

(defn batch-actors-ops [actors]
  (let [counts (map #(count (:movies %)) actors)
        ids (reductions (comp inc +) 0 counts)
        ops (mapcat actor-to-op actors ids)]
    (doall ops)))

(defn movie-title [line]
  (let [tab (. line (indexOf "\t"))]
    (when (not= tab -1)
      (. line (substring 0 tab) trim))))

(defn movie-line? [line]
  (and
    (not (empty? line))
    (not (.startsWith line char-quote))    ; Not a TV series
    ; (not= -1 (.indexOf line char-tab))    ; Has tab(s)
    (= -1 (.indexOf line "{{SUSPENDED}}")) ; Not bad data
    (= -1 (.indexOf line "V)"))))          ; Not TV movie or straight to video

(defn role-line? [line]
  (and
    (movie-line? line)
    (not= -1 (.indexOf line ")")) ))

(defn split-by [pred coll]
  (let [remove-sep (fn [el] ((complement pred) (first el)))]
    (filter remove-sep (partition-by pred coll))))

(defn store-movies [batch]
  (let [titles (map (fn [b] { :title b }) batch)]
    (create-batch-and-index titles "Movie" :title)))

(defn parse-movies [lines]
  (let [titles (filter identity (map movie-title (filter movie-line? lines)))]
    (doseq [batch (partition-all *batch-size* titles)]
      (print ".")
      (flush)
      (store-movies batch))
    (println "done")))

(defn extract-role [role-line]
  (let [paren (. role-line (indexOf ")"))]
    (. role-line (substring 0 (inc paren)) trim)))

(defn parse-actor [[actor-line & roles]]
  (let [[actor title & rest] (clojure.string/split actor-line #"\t+")
        roles (map extract-role (filter role-line? (map #(.trim %) (conj roles title))))
        movies (filter identity roles)]
    (when (not (empty? movies))
      { :actor actor :movies movies })))

(defn parse-actors [lines]
  (let [actors-lines (split-by empty? (drop 3 lines))
        actors-and-roles (filter identity (map parse-actor actors-lines))
        batches (partition-all *batch-size* actors-and-roles)]
    (doseq [batch batches]
      (http/with-connection-pool { :threads 3 }
        (perform (batch-actors-ops batch))
        (print ".") (flush)))
    (println "done")))

(defn load-file-with-parser
  [file parser & {:keys [start-at]}]
  (with-open [in (io/reader
                   (java.util.zip.GZIPInputStream.
                     (io/input-stream file)))]
    (let [lines (line-seq in)]
      (loop [[line & lines] lines
             state { :header true }]
        (if (:header state)
          (recur lines { :header (not= line start-at) })
          (parser lines))))))

(defn -main [& args]
  (nr/connect! url)
  (println "Loading movies...")
  (load-file-with-parser "resources/movies.list.gz" parse-movies :start-at "MOVIES LIST")
  (println "Loading actors...")
  (load-file-with-parser "resources/actors.list.gz" parse-actors :start-at "THE ACTORS LIST")
  (println "Loading actresses...")
  (load-file-with-parser "resources/actresses.list.gz" parse-actors :start-at "THE ACTRESSES LIST"))
