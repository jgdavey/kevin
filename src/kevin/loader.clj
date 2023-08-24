(ns kevin.loader
  "To use, download the movies, actors and actresses lists from a mirror on
  http://www.imdb.com/interfaces, and copy them (still zipped) to the data
  folder. You can then run `lein run -m kevin.loader`"
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async :refer [chan go >! <! close!]]
            [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [cognitect.transit :as t]
            [charred.api :as charred]
            [datomic.api :as d :refer [q db]]
            [kevin.util :as util]))

(defonce conn nil)

;; Don't change after an import
(def batch-size 1000)

(def conn-uri "datomic:ddb-local://localhost:8779/local/kevin")

(def BATCHID :kevin.initial-import/batch-id)

(def import-schema
  [{:db/ident BATCHID
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value}])

(defn write-batch [path data]
  (with-open [f (io/output-stream path)]
    (let [w (t/writer f :msgpack)]
      (t/write w data))))

(defn connect! []
  (when conn
    (d/release conn))
  (System/setProperty "aws.accessKeyId" "dummy")
  (System/setProperty "aws.secretKey" "dummy")
  (d/create-database conn-uri)
  (alter-var-root #'conn
                  (constantly
                   (d/connect conn-uri))))

(defn dot
  "Returns a passthru transducer that prints a dot for progress
tracking."
  ([]
   (map
    (fn [x]
      (print ".") (flush)
      x)))
  ([n]
   (let [c (volatile! 0)]
     (map
      (fn [x]
        (when (zero? (mod (vswap! c unchecked-inc) n))
          (print ".") (flush))
        x)))))

(def genres
  {"Action"      :title.genre/action
   "Adult"       :title.genre/adult
   "Adventure"   :title.genre/adventure
   "Animation"   :title.genre/animation
   "Comedy"      :title.genre/comedy
   "Crime"       :title.genre/crime
   "Documentary" :title.genre/documentary
   "Drama"       :title.genre/drama
   "Family"      :title.genre/family
   "Fantasy"     :title.genre/fantasy
   "Film-Noir"   :title.genre/film-noir
   "Game-Show"   :title.genre/game-show
   "Horror"      :title.genre/horror
   "Music"       :title.genre/music
   "Musical"     :title.genre/musical
   "Mystery"     :title.genre/mystery
   "News"        :title.genre/news
   "Reality-TV"  :title.genre/reality-tv
   "Romance"     :title.genre/romance
   "Sci-Fi"      :title.genre/sci-fi
   "Short"       :title.genre/short
   "Sport"       :title.genre/sport
   "Talk-Show"   :title.genre/talk-show
   "Thriller"    :title.genre/thriller
   "War"         :title.genre/war
   "Western"     :title.genre/western})

(defn movie-title [^String line]
  (let [tab (. line (indexOf "\t"))]
    (when (not= tab -1)
      (.. line (substring 0 tab) trim))))

(defn extract-year [^String year]
  (try
    (Integer. year)
    (catch Exception _
      nil)))

(defn parse-genres [gstr]
  (into []
        (keep genres)
        (str/split (str gstr) #",")))

(def title-types
  {"movie"        :title.type/movie
   "short"        :title.type/short
   "tvEpisode"    :title.type/tv.episode
   "tvMiniSeries" :title.type/tv.miniseries
   "tvMovie"      :title.type/tv.movie
   "tvShort"      :title.type/tv.short
   "tvSpecial"    :title.type/tv.special
   "video"        :title.type/video
   "videoGame"    :title.type/videogame})

(defn title-tx [[tconst type title _ adult year _ _ genres]]
  (let [type (get title-types type)
        tconst (not-empty tconst)
        title (not-empty title)]
    (when (and type tconst title)
      (let [gen (if (= adult "1")
                  [:title.genre/adult]
                  (parse-genres genres))
            year (extract-year year)]
        (cond-> {:db/id tconst
                 :title/tconst tconst
                 :title/type type
                 :title/title title}
          (seq gen) (assoc :title/genre gen)
          year (assoc :title/year year))))))

(defn person-tx [[nconst name & _]]
  (let [nconst (not-empty nconst)
        name (not-empty name)]
    (when (and nconst name)
      {:db/id nconst
       :person/nconst nconst
       :person/name name})))

(defn role-tx [[tconst _ nconst & _]]
  (let [nconst (not-empty nconst)
        tconst (not-empty tconst)]
    (when (and nconst tconst)
      [:db/add [:person/nconst nconst] :person/roles [:title/tconst tconst]])))

(defn- generate-batch-files [file name offset line-tx-xform]
  (io/make-parents "data" "batches" name "_")
  (let [out-ch   (chan 1)
        batch-ch (chan 10)
        done-ch  (chan 1)
        batch-number (atom 0)
        stop (fn stop []
               (async/close! batch-ch))
        handle-ex (fn [ex]
                    (prn ex)
                    (stop)
                    nil)]
    (async/pipeline-blocking
     3
     out-ch
     (map (fn [data]
            (let [n (swap! batch-number inc)]
              (write-batch (format "data/batches/%s/%s" name n)
                           {:batch-ident {:db/id "datomic.tx"
                                          BATCHID (str name "-" n)}
                            :data (into [] line-tx-xform data)})
              n)))
     batch-ch
     true
     handle-ex)
    (go
      (loop [total 0]
        (when (zero? (mod total 100))
          (print ".") (flush))
        (if-let [_ (<! out-ch)]
          (recur (inc total))
          (>! done-ch {:completed total}))))
    (doseq [batch (->> file
                       io/file
                       charred/read-csv
                       (partition-all batch-size)
                       (drop offset))]
      (async/>!! batch-ch batch))
    (close! batch-ch)
    (async/<!! done-ch)))

(defn batches [subdir]
  (->> (io/file (str "data/batches/" subdir))
       .listFiles
       (keep #(.getName %))))

(defn- batch-offset [subdir]
  (->> (batches subdir)
       (keep parse-long)
       (sort >)
       first))

(defn generate-titles-batches []
  (let [offset (or (batch-offset "titles") 0)]
    (println (str "\n\nLoading titles starting with batch " offset))
    (generate-batch-files "data/titles.csv" "titles" offset (keep title-tx))))

(defn generate-people-batches []
  (let [offset (or (batch-offset "people") 0)]
    (println (str "\n\nLoading people starting with batch " offset))
    (generate-batch-files "data/person.csv" "people" offset (keep person-tx))))

(defn generate-roles-batches []
  (let [offset (or (batch-offset "roles") 0)]
    (println (str "\n\nLoading roles starting with batch " offset))
    (generate-batch-files "data/roles.csv" "roles" offset (comp
                                                           (filter #(= (nth % 4) "\\N"))
                                                           (keep role-tx)))))

(defn batches-already-loaded []
  (into #{}
        (d/qseq {:query '[:find [?batch ...]
                          :in $ ?batch-attr
                          :where [_ ?batch-attr ?batch]]
                 :args [(d/db conn) BATCHID]})))

(defn batches-generated [subdir]
  (into [] (map (fn [n]
                  {:batch-id (str subdir "-" n)
                   :path (str "data/batches/" subdir "/" n)}))
        (->> (batches subdir)
             (keep parse-long)
             (sort))))

(defn load-batchfile [path]
  (with-open [f (io/input-stream path)]
    (let [r (t/reader f :msgpack)
          {:keys [batch-ident data]} (t/read r)]
      @(d/transact-async conn (cons batch-ident data)))))

(defn drain
  "Close ch and discard all items on it. Returns nil."
  [ch]
  (close! ch)
  (async/go-loop []
    (when (<! ch) (recur)))
  nil)

(defn load-batchfiles [subdir opts]
  (println (str "\n\nLoading " subdir))
  (let [parallelism (or (:jobs opts) 2)
        loaded (batches-already-loaded)
        tx-result-ch (async/chan parallelism)
        batches-ch (async/to-chan! (batches-generated subdir))
        load-xf (comp (remove #(.contains loaded (:batch-id %)))
                      (map :path)
                      (map load-batchfile)
                      (dot 10))
        stop (fn stop []
               (drain batches-ch))
        handle-ex (fn [ex]
                    (prn ex)
                    (stop)
                    nil)]
    (async/pipeline-blocking
     parallelism
     tx-result-ch
     load-xf
     batches-ch
     true
     handle-ex)
    (async/<!!
     (async/transduce
      (halt-when :cognitect.anomalies/category (fn [result bad-input]
                                                 (drain tx-result-ch)
                                                 (drain batches-ch)
                                                 (assoc bad-input :completed result)))
      (completing (fn [m {:keys [tx-data]}]
                    (-> m (update :txes inc) (update :datoms + (count tx-data)))))
      {:txes 0 :datoms 0}
      tx-result-ch))))

(defn load-titles []
  (load-batchfiles "titles" {}))

(defn load-people []
  (load-batchfiles "people" {}))

(defn load-roles []
  (load-batchfiles "roles" {}))

(defn batch-for-each-person-eid [batches xform]
  (let [out-ch   (chan 1)
        tx-ch    (chan 10 (partition-all batch-size))
        batch-ch (chan 100)
        done-ch  (chan 1)
        stop (fn stop []
               (async/close! batch-ch)
               (async/close! tx-ch))
        handle-ex (fn [ex]
                    (prn ex)
                    (stop)
                    nil)]
    (async/pipeline-blocking
     4
     out-ch
     (map (fn [batch]
            @(d/transact-async conn batch)))
     tx-ch
     true
     handle-ex)
    (async/pipeline
     8
     tx-ch
     xform
     batch-ch
     true
     handle-ex)
    (go
      (loop [total 1]
        (when (zero? (mod total 100))
          (print ".") (flush))
        (if-let [_ (<! out-ch)]
          (recur (inc total))
          (>! done-ch {:completed total}))))
    (async/onto-chan! batch-ch batches)
    (async/<!! done-ch)))

(defn reify-collaborators []
  (let [d (db conn)]
    (batch-for-each-person-eid
     (partition-all 100
                    (d/qseq
                     {:query
                      '[:find [?p ...]
                        :where [?p :person/nconst _]] :args [d]}))
     (comp cat
           (map (fn [p1]
                  (d/qseq
                   {:query
                    '[:find ?p1 ?p2
                      :in $ % ?p1
                      :where
                      [?p1 :person/roles ?m]
                      [?p2 :person/roles ?m]
                      [(not= ?p1 ?p2)]
                      (non-ignorable? ?m)]
                    :args [d util/rules p1]})))
           cat
           (map (fn [[p1 p2]]
                  [:db/add p1 :person/collaborators p2]))))))

(defn import! [{:keys [generate? load? reify-collaborators?]}]
  (connect!)
  (d/transact conn (read-string (slurp "resources/schema.edn")))
  (d/transact conn import-schema)
  (when generate?
    (generate-titles-batches)
    (generate-people-batches)
    (generate-roles-batches))
  (when load?
    (load-titles)
    (load-people)
    (load-roles))
  (when reify-collaborators?
    (reify-collaborators)))


(def cli-options
  ;; An option with a required argument
  [["-g" "--[no-]generate" "Generate batch files?"
    :id :generate?
    :default true]
   ["-l" "--[no-]load" "Load into Datomic?"
    :id :load?
    :default true]
   ["-r" "--[no-]reify" "Reify collaborators?"
    :id :reify-collaborators?
    :default false]
   ;; A boolean option defaulting to nil
   ["-h" "--help"]])

(defn -main [& args]
  (System/setProperty "datomic.txTimeoutMsec" "30000") ;; 30 seconds
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (when (:help options)
      (println summary)
      (System/exit 0))

    (when (or (seq errors) (seq arguments))
      (doseq [error errors]
        (println (str "ERROR: " error)))
      (doseq [arg arguments]
        (println (str "ERROR: extra argument `" arg "'")))
      (println)
      (println summary)
      (System/exit 1))
    (import! options))

  (shutdown-agents)
  (Thread/sleep 1000)
  (System/exit 0))

(comment
  (connect!)
  (async/<!! (load-titles))

  :ok)
