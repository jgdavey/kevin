(ns kevin.loader
  "To use, download the movies, actors and actresses lists from a mirror on
  http://www.imdb.com/interfaces, and copy them (still zipped) to the resources
  folder. You can then run `lein run -m kevin.loader`"
  (:require [clojure.java.io :as io]
            [datomic.api :as d :refer [q db]]))

(def uri "datomic:dev://localhost:4334/movies")
(def schema (read-string (slurp "resources/schema.edn")))
(def ^:dynamic *batch-size* 500)

(defn ensure-schema [conn]
  (or (-> conn d/db (d/entid :actor/name))
      @(d/transact conn schema)))

(defn ensure-db [db-uri]
  (let [newdb? (d/create-database db-uri)
        conn (d/connect db-uri)]
    (ensure-schema conn)
    conn))

(def conn (ensure-db uri))

(def char-quote "\"")
(def char-tab "\t")

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
  (let [titles (map (fn [b] { :db/id (d/tempid :db.part/user) :movie/title b }) batch)]
    @(d/transact conn titles)))

(def actor-query '[:find ?e :in $ ?name :where [?e :actor/name ?name]])

(defn actor-tx-data
  ([a] (actor-tx-data (db conn) a))
  ([d {:keys [actor movies]}]
    (when (zero? (count (q actor-query d actor)))
      (let [actor-id  (d/tempid :db.part/user)
            movie-txs (map (fn [m] {:movie/title m
                                :db/id (d/tempid :db.part/user)
                                :_movies actor-id
                                }) movies)
            actor-tx  { :db/id actor-id, :actor/name actor }]
      (concat [actor-tx] movie-txs)))))

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
  (let [actors-and-roles (->> lines
                              (drop 3)
                              (split-by empty?)
                              (map parse-actor)
                              (filter identity))]
    (doseq [batch (partition-all *batch-size* actors-and-roles)]
      (let [d (db conn)]
        @(d/transact conn (mapcat (partial actor-tx-data d) batch)))
      (print ".") (flush))
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
  ; (println "Loading movies...")
  ; (load-file-with-parser "resources/movies.list.gz" parse-movies :start-at "MOVIES LIST")
  (println "Loading actors...")
  (load-file-with-parser "resources/actors.list.gz" parse-actors :start-at "THE ACTORS LIST")
  (println "Loading actresses...")
  (load-file-with-parser "resources/actresses.list.gz" parse-actors :start-at "THE ACTRESSES LIST")
)
