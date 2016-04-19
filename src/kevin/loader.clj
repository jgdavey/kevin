(ns kevin.loader
  "To use, download the movies, actors and actresses lists from a mirror on
  http://www.imdb.com/interfaces, and copy them (still zipped) to the data
  folder. You can then run `lein run -m kevin.loader`"
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async :refer [chan go >! <! close!]]
            [datomic.api :as d :refer [q db]]
            [kevin.core :refer [eids-with-attr-val]]
            [kevin.system :as system]))

(System/setProperty "datomic.txTimeoutMsec" "30000") ;; 30 seconds

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

(defn movie-tx [^String title]
  (let [tx {:db/id (d/tempid :db.part/user)
            :movie/title title}]
    (if-let [year (extract-year title)]
      (assoc tx :movie/year year)
      tx)))

(defn actor-movie-tx [actor-id title]
  {:movie/title title
   :db/id (d/tempid :db.part/user)
   :actor/_movies actor-id})

(defn actor-tx [tuples]
  (let [actor-id (d/tempid :db.part/user)]
    (concat [{:db/id actor-id :person/name (ffirst tuples)}]
            (map (fn [[_ movie]]
                   (actor-movie-tx actor-id movie)) tuples))))

(defn retract-roles [d {:keys [actor movies]}]
  (->> (q '[:find ?actor ?movie
            :in $ ?name [?title ...]
            :where
            [?actor :person/name ?name]
            [?actor :actor/movies ?movie]
            [?movie :movie/title ?title]]
          d actor movies)
       (map (fn [[actor movie]]
              [:db/retract actor :actor/movies movie]))))

(defn parse-genre [^String line]
  (map #(.trim ^String %) (clojure.string/split line #"\t+")))

(defn genre-tx [line]
  (let [[title genre] (parse-genre line)]
    (when-let [g (genres genre)]
      {:db/id (d/tempid :db.part/user)
       :movie/title title
       :movie/genre g})))

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

(defmacro ensure-transformed-file
  "in and out are bound for you"
  [[file outfile] & body]
  `(when-not (.exists (io/as-file ~outfile))
     (with-open [~'in (io/reader
                      (java.util.zip.GZIPInputStream. (io/input-stream ~file))
                      :encoding "ISO-8859-1")
                 ~'out (io/writer ~outfile)]
       ~@body)))

(defn ensure-transformed-movies [file outfile]
  (ensure-transformed-file [file outfile]
    (loop [[line & lines] (drop-while #(not= % "MOVIES LIST") (line-seq in))]
      (when line
        (when (movie-line? line)
          (when-let [title (movie-title line)]
            (doto out
              (.write title)
              (.newLine))))
        (recur lines)))))

(defn ensure-transformed-actors [file outfile & {:keys [start-at]}]
  (ensure-transformed-file [file outfile]
    (loop [lines (drop 3 (drop-while #(not= % start-at) (line-seq in)))]
      (let [[actor-lines lines] (split-with (complement empty?) (rest lines))]
        (when (seq actor-lines)
          (when-let [actor-data (try (parse-actor actor-lines) (catch Throwable t nil))]
            (let [{:keys [actor movies]} actor-data]
              (doseq [movie movies]
                (doto out
                  (.write actor)
                  (.write char-tab)
                  (.write movie)
                  (.newLine)))))
          (recur lines))))))

(defn ensure-transformed-genres [file outfile]
  (ensure-transformed-file [file outfile]
    (loop [[line & lines] (drop 3 (drop-while #(not= % "8: THE GENRES LIST") (line-seq in)))]
      (when line
        (when (movie-line? line)
          (let [[title genre] (parse-genre line)]
            (doto out
              (.write title)
              (.write char-tab)
              (.write genre)
              (.newLine))))
        (recur lines)))))

(defn batch
  "Returns a channel that batches entries from in"
  [in timeout-ms]
  (let [inner (chan 1)
        splitter? (partial identical? ::split)
        proc (go (loop [t (async/timeout timeout-ms)]
                   (let [[v c] (async/alts! [t in])]
                     (condp identical? c
                       t (do (>! inner ::split)
                             (recur (async/timeout timeout-ms)))
                       in (if (nil? v)
                            (close! inner)
                            (do (>! inner v)
                                (recur t)))))))
        out (->> (async/partition-by splitter? inner)
                 (async/remove< (comp splitter? first)))]
    out))

(defn transact-all
  "Returns a chan"
  [tx-chan transact n]
  (let [procs (map (fn [_] (go (loop []
                                 (when-let [batch (<! tx-chan)]
                                   (print ".")
                                   (flush)
                                   (transact batch)
                                   (recur)))))
                   (range n))
        control-chan (async/merge procs)]
    (go (while (<! control-chan) true))))

(defn load-movies []
  (let [work-chan    (chan 128)
        tx-data-chan (chan 1)
        tx-chan (batch tx-data-chan 50)
        control-chan (transact-all tx-chan (comp deref (partial d/transact conn)) 4)]
    (dotimes [i 8]
      (go (loop []
            (if-let [line (<! work-chan)]
              (do
                (>! tx-data-chan (movie-tx line))
                (recur))
              (close! tx-data-chan)))))
    (with-open [file (io/reader "data/movies.transformed")]
      (doseq [line (line-seq file)]
        (async/>!! work-chan line)))
    (close! work-chan)
    (async/<!! control-chan)))

(defn load-actors-from [file]
  (let [line-chan  (chan 128)
        actor-chan (async/partition-by first line-chan)
        tx-chan (chan 1)
        batched-tx-chan (batch tx-chan 50)
        control-chan (transact-all batched-tx-chan
                                   (fn [batch]
                                     @(d/transact conn (apply concat batch)))
                                   4)]
    (dotimes [i 8]
      (go (loop []
            (if-let [lines (<! actor-chan)]
              (do
                (>! tx-chan (actor-tx lines))
                (recur))
              (close! tx-chan)))))
    (with-open [file (io/reader file)]
      (doseq [line (line-seq file)]
        (async/>!! line-chan (clojure.string/split line #"\t+"))))
    (close! line-chan)
    (async/<!! control-chan)))

(defn load-actors []
  (load-actors-from "data/actors.transformed"))

(defn load-actresses []
  (load-actors-from "data/actresses.transformed"))

(defn load-genres []
  (let [work-chan    (chan 128)
        tx-data-chan (chan 1)
        tx-chan (batch tx-data-chan 50)
        control-chan (transact-all tx-chan (comp deref (partial d/transact conn)) 4)]
    (dotimes [i 10]
      (go (loop []
            (if-let [line (<! work-chan)]
              (do
                (when-let [tx (genre-tx line)]
                  (>! tx-data-chan tx))
                (recur))
              (close! tx-data-chan)))))
    (with-open [file (io/reader "data/genres.transformed")]
      (doseq [line (line-seq file)]
        (async/>!! work-chan line)))
    (close! work-chan)
    (async/<!! control-chan)))

(defn -main [& args]
  (println "\nTransforming files for faster load...")
  (ensure-transformed-movies "data/movies.list.gz" "data/movies.transformed")
  (ensure-transformed-actors "data/actors.list.gz" "data/actors.transformed" :start-at "THE ACTORS LIST")
  (ensure-transformed-actors "data/actresses.list.gz" "data/actresses.transformed" :start-at "THE ACTRESSES LIST")
  (ensure-transformed-genres "data/genres.list.gz" "data/genres.transformed")
  (let [system (system/start (system/system))]
    (alter-var-root #'conn (constantly (:conn (:db system))))
    (time (do
      (println "\nLoading movies...")
      (load-movies)
      (println "\nLoading actors...")
      (load-actors)
      (println "\nLoading actresses...")
      (load-actresses)
      (println "\nLoading genres...")
      (load-genres)))

    (system/stop system)))
