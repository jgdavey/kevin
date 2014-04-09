(ns kevin.loader
  "To use, download the movies, actors and actresses lists from a mirror on
  http://www.imdb.com/interfaces, and copy them (still zipped) to the data
  folder. You can then run `lein run -m kevin.loader`"
  (:gen-class)
  (:require [clojure.java.io :as io]
            [datomic.api :as d :refer [q db]]
            [kevin.core :refer [eids-with-attr-val]]
            [kevin.system :as system]))

(def conn nil)
(def system (system/system))
(def ^:dynamic *batch-size* 500)

(def char-quote "\"")
(def char-tab "\t")

(def genres
  {"Action"      :movie.genre/action
   "Adult"       :movie.genre/adult
   "Adventure"   :movie.genre/adventure
   "Animation"   :movie.genre/animation
   "Comedy"      :movie.genre/comedy
   "Crime"       :movie.genre/crime
   "Documentary" :movie.genre/documentary
   "Drama"       :movie.genre/drama
   "Family"      :movie.genre/family
   "Fantasy"     :movie.genre/fantasy
   "Film-Noir"   :movie.genre/film-noir
   "Horror"      :movie.genre/horror
   "Musical"     :movie.genre/musical
   "Mystery"     :movie.genre/mystery
   "Romance"     :movie.genre/romance
   "Sci-Fi"      :movie.genre/sci-fi
   "Short"       :movie.genre/short
   "Thriller"    :movie.genre/thriller
   "War"         :movie.genre/war
   "Western"     :movie.genre/western})

(defn movie-title [^String line]
  (let [tab (. line (indexOf "\t"))]
    (when (not= tab -1)
      (.. line (substring 0 tab) trim))))

(defn extract-year [^String movie-title]
  (if-let [^String year (last (re-find #"\((\d\d\d\d).*\)$" movie-title))]
    (Integer. year)))

(defn add-year [title]
  (if-let [year (extract-year title)]
    {:db/id [:movie/title title] :movie/year year}))

(defn add-years-to-movies [conn]
  (let [tx-data (->> (q '[:find ?t
                          :where [?e :movie/title ?t]]
                        (db conn))
                     (map first)
                     (map add-year)
                     (filter identity))]
    (doseq [batch (partition-all *batch-size* tx-data)]
      (print ".")
      (flush)
      @(d/transact conn batch))
    :ok))

(defn movie-line? [^String line]
  (and
    (not (empty? line))
    (not (.startsWith line char-quote))    ; Not a TV series
    (= -1 (.indexOf line "{{SUSPENDED}}")) ; Not bad data
    (= -1 (.indexOf line "(VG)"))          ; Not a videogame
    (= -1 (.indexOf line "V)"))))          ; Not TV movie or straight to video

(defn role-line? [^String line]
  (and
    (movie-line? line)
    (not= -1 (.indexOf line ")"))))

(defn legit-role? [^String line]
  (and
    (= -1 (.indexOf line "(archive footage)"))
    (= -1 (.indexOf line "(unconfirmed)"))
    (= -1 (.indexOf line "(archival"))))

(defn split-by [pred coll]
  (let [remove-sep (fn [el] ((complement pred) (first el)))]
    (filter remove-sep (partition-by pred coll))))

(defn store-movies [batch]
  (let [titles (map (fn [b]
                      (let [tx {:db/id (d/tempid :db.part/user)
                                :movie/title b}]
                        (if-let [year (extract-year b)]
                          (assoc tx :movie/year year)
                          tx)))
                    batch)]
    @(d/transact conn titles)))

(defn actor-tx-data
  ([a] (actor-tx-data (db conn) a))
  ([d {:keys [actor movies]}]
    (when-not (d/entid d [:person/name actor])
      (let [actor-id  (d/tempid :db.part/user)
            movie-txs (map (fn [m] {:movie/title m
                                :db/id (d/tempid :db.part/user)
                                :actor/_movies actor-id
                                }) movies)
            actor-tx  { :db/id actor-id, :person/name actor }]
      (concat [actor-tx] movie-txs)))))

(defn retract-roles
  ([a] (actor-tx-data (db conn) a))
  ([d {:keys [actor movies]}]
   (->> (q '[:find ?actor ?movie
             :in $ ?name [?title ...]
             :where
             [?actor :person/name ?name]
             [?actor :actor/movies ?movie]
             [?movie :movie/title ?title]]
           d actor movies)
        (map (fn [[actor movie]]
               [:db/retract actor :actor/movies movie])))))

(defn parse-genre [db ^String line]
  (let [[title genre] (map #(.trim ^String %) (clojure.string/split line #"\t+"))]
    (when-let [g (genres genre)]
      {:db/id (d/tempid :db.part/user)
       :movie/title title
       :movie/genre g})))

(defn parse-genres [lines]
  (let [db (db conn)
        tx-data (filter identity (map (partial parse-genre db) (filter movie-line? lines)))]
     (doseq [batch (partition-all *batch-size* tx-data)]
       (print ".")
       (flush)
       @(d/transact-async conn (doall batch)))
    :ok))

(defn parse-movies [lines]
  (let [titles (filter identity (map movie-title (filter movie-line? lines)))]
    (doseq [batch (partition-all *batch-size* titles)]
      (print ".")
      (flush)
      (store-movies batch))
    (println "done")))

(defn extract-role [^String role-line]
  (let [paren (. role-line (indexOf ")"))]
    (.. role-line (substring 0 (inc paren)) trim)))

(defn extract-potential-roles [[actor-line & role-lines]]
  (let [[actor title & rest] (clojure.string/split actor-line #"\t+")]
    {:actor actor
     :movies (map #(.trim ^String %) (conj role-lines title))}))

(defn parse-actor [lines]
  (let [{actor :actor potential-roles :movies} (extract-potential-roles lines)
        roles (map extract-role (filter #(and (role-line? %) (legit-role? %)) potential-roles))
        movies (filter identity roles)]
    (when (and (seq movies) actor)
      { :actor actor :movies movies })))

(defn actors-and-roles
  ([lines]
   (actors-and-roles lines parse-actor))
  ([lines parse-fn]
   (->> lines
        (drop 3)
        (split-by empty?)
        (filter identity)
        (map parse-fn)
        (filter identity))))

(defn parse-bogus-roles [lines]
  (let [{actor :actor potential-roles :movies} (extract-potential-roles lines)
        roles (map extract-role (filter #(and (role-line? %) (not (legit-role? %))) potential-roles))
        movies (filter identity roles)]
    (when (and (seq movies) actor)
      { :actor actor :movies movies })))

(defn retract-bogus-roles [lines]
  (try
    (let [actor-roles (actors-and-roles lines parse-bogus-roles)]
      (doseq [batch (partition-all *batch-size* actor-roles)]
        (let [d (db conn)
              tx (mapcat (partial retract-roles d) batch)]
          (if (empty? tx)
            (print "-")
            (do (d/transact-async conn tx)
                (print "."))))
        (flush)))
    (catch Exception e))
  (println "done"))

(defn parse-actors [lines]
  (try
    (let [actor-roles (actors-and-roles lines)]
      (doseq [batch (partition-all *batch-size* actor-roles)]
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
    (println "Loading genres...")
    (load-file-with-parser "data/genres.list.gz" parse-genres :start-at "8: THE GENRES LIST")
  ))
