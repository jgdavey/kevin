(ns kevin.loader
  "To use, download the movies, actors and actresses lists from a mirror on
  http://www.imdb.com/interfaces, and copy them (still zipped) to the data
  folder. You can then run `lein run -m kevin.loader`"
  (:gen-class)
  (:require [clojure.java.io :as io]
            [datomic.api :as d :refer [q db]]
            [kevin.core :refer [actor-name->eid]]
            [kevin.system :as system]))

(def conn nil)
(def system (system/system))
(def ^:dynamic *batch-size* 500)

(def char-quote "\"")
(def char-tab "\t")

(defn movie-title [line]
  (let [tab (. line (indexOf "\t"))]
    (when (not= tab -1)
      (.. line (substring 0 tab) trim))))

(defn extract-year [movie-title]
  (Integer. (last (re-find #"\((\d\d\d\d).*\)$" movie-title))))

(defn add-year [title]
  {:db/id [:movie/title title]
   :movie/year (extract-year title)})

(defn add-years-to-movies [conn]
  (->> (q '[:find ?t
       :where [?e :movie/title ?t]]
         (db conn))
      (map first)
      (map add-year)
      (d/transact conn)
       deref))

(defn movie-line? [line]
  (and
    (not (empty? line))
    (not (.startsWith line char-quote))    ; Not a TV series
    (= -1 (.indexOf line "{{SUSPENDED}}")) ; Not bad data
    (= -1 (.indexOf line "(VG)"))          ; Not a videogame
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

(defn actor-tx-data
  ([a] (actor-tx-data (db conn) a))
  ([d {:keys [actor movies]}]
    (when-not (actor-name->eid d actor)
      (let [actor-id  (d/tempid :db.part/user)
            movie-txs (map (fn [m] {:movie/title m
                                :db/id (d/tempid :db.part/user)
                                :actor/_movies actor-id
                                }) movies)
            actor-tx  { :db/id actor-id, :person/name actor }]
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
    (.. role-line (substring 0 (inc paren)) trim)))

(defn parse-actor [[actor-line & roles]]
  (let [[actor title & rest] (clojure.string/split actor-line #"\t+")
        roles (map extract-role (filter role-line? (map #(.trim %) (conj roles title))))
        movies (filter identity roles)]
    (when (and (not (empty? movies)) actor)
      { :actor actor :movies movies })))

(defn parse-actors [lines]
  (try
    (let [actors-and-roles (->> lines
                                (drop 3)
                                (split-by empty?)
                                (filter identity)
                                (map parse-actor)
                                (filter identity))]
      (doseq [batch (partition-all *batch-size* actors-and-roles)]
        (let [d (db conn)
              tx (mapcat (partial actor-tx-data d) batch)]
          (if (empty? tx)
            (print "-")
            (do (d/transact-async conn tx)
                (print "."))))
        (flush)))
    (catch Exception e))
  (println "done"))

(defn load-file-with-parser
  [file parser & {:keys [start-at]}]
  (with-open [in (io/reader
                   (java.util.zip.GZIPInputStream. (io/input-stream file))
                   :encoding "ISO-8859-1")]
      (let [lines (line-seq in)]
        (loop [[line & lines] lines
              state { :header true }]
          (if (:header state)
            (recur lines { :header (not= line start-at) })
            (parser lines))))))

(defn -main [& args]
  (let [system (system/start (system/system))]
    (alter-var-root #'conn (constantly (:conn (:db system))))
    (println "Loading movies...")
    (load-file-with-parser "data/movies.list.gz" parse-movies :start-at "MOVIES LIST")
    (println "Loading actors...")
    (load-file-with-parser "data/actors.list.gz" parse-actors :start-at "THE ACTORS LIST")
    (println "Loading actresses...")
    (load-file-with-parser "data/actresses.list.gz" parse-actors :start-at "THE ACTRESSES LIST")
  ))
