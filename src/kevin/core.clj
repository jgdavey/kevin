(ns kevin.core
  (:require [datomic.api :as d :refer [q]]
            [clojure.string :as str]
            [kevin.util :as util]
            [kevin.search :refer [bidirectional-bfs]])
  (:import datomic.Datom))

(defprotocol Eid
  (e [_]))

(extend-protocol Eid
  java.lang.Long
  (e [i] i)

  datomic.Entity
  (e [ent] (:db/id ent)))

(def acted-with-rules
  '[[(acted-with ?e1 ?e2 ?path)
     [?e1 :person/roles ?m]
     [?e2 :person/roles ?m]
     [(!= ?e1 ?e2)]
     [(vector ?e1 ?m ?e2) ?path]]
    [(acted-with-1 ?e1 ?e2 ?path)
     (acted-with ?e1 ?e2 ?path)]
    [(acted-with-2 ?e1 ?e2 ?path)
     (acted-with ?e1 ?x ?pp)
     (acted-with ?x ?e2 ?p2)
     [(butlast ?pp) ?p1]
     [(concat ?p1 ?p2) ?path]]
    [(acted-with-3 ?e1 ?e2 ?path)
     (acted-with-2 ?e1 ?x ?pp)
     (acted-with ?x ?e2 ?p2)
     [(butlast ?pp) ?p1]
     [(concat ?p1 ?p2) ?path]]
    [(acted-with-4 ?e1 ?e2 ?path)
     (acted-with-3 ?e1 ?x ?pp)
     (acted-with ?x ?e2 ?p2)
     [(butlast ?pp) ?p1]
     [(concat ?p1 ?p2) ?path]]])

(defn actor-or-movie-name [db eid]
  (let [ent (d/entity db (e eid))]
    (or (:title/title ent) (:person/name ent))))

(defn actor-search
  "Returns set with exact match, if found. Otherwise query will
  be formatted with format-query passed as-is to Lucene"
  [db query]
  (if (str/blank? query)
    []
    (q '[:find ?e ?name ?score (count ?m)
         :keys id name :score roles
         :in $ ?search
         :where
         [(fulltext $ :person/name ?search) [[?e ?name _ ?score]]]
         [?e :person/roles ?m]]
       db (util/format-query query))))

(defn movie-actors
  "Given a datomic database value and a movie id,
  returns ids for actors in that movie."
  [db eid]
  (map :e (d/datoms db :vaet eid :person/roles)))

(defn actor-movies
  "Given a datomic database value and an actor id,
  returns ids for movies that actor was in."
  [db eid]
  (not-empty
   (map :v (d/datoms db :eavt eid :person/roles))))

(defn collaborators
  [db eid]
  (not-empty
   (map :v
        (d/datoms db :eavt eid :person/collaborators))))

(defn collaborations [db p1 p2]
  (q '[:find [(pull ?m [:db/id
                        :title/title
                        :title/year
                        {:title/type [:db/ident]}
                        {:title/genre [:db/ident]}]) ...]
       :in $ % ?p1 ?p2
       :where
       [?p1 :person/roles ?m]
       [?p2 :person/roles ?m]
       (non-ignorable? ?m)]
     db util/rules p1 p2))

(defn neighbors
  "db is database value
  eid is an actor or movie eid"
  [db eid]
  (or (seq (actor-movies db (e eid)))
      (seq (movie-actors db (e eid)))))

(defn search [db start end]
  (let [s #(sort-by :score > (actor-search db %))
        starts (s start)
        ends (s end)]
    (for [p1 starts, p2 ends]
      [p1 p2])))

(defn path-at-depth [db source target depth]
  (let [rule (symbol (str "acted-with-" depth))]
    (q (concat '[:find ?path
                 :in $ % ?actor ?target
                 :where]
               [(list rule '?actor '?target '?path)])
      db acted-with-rules source target)))

(defn ascending-years? [annotated-node]
  (if-let [years (->> annotated-node
                             (map :year)
                             (filter identity)
                             seq)]
    (apply <= years)
    true))

(defn is-documentary? [entity]
  (let [genres (:title/genre entity)]
    (and genres (contains? genres :title.genre/documentary))))

(defn without-documentaries
  "Returns a function suitable for use with datomic.api/filter"
  [db]
  (let [movies-attr (d/entid db :person/roles)
        has-documentaries? (fn [db ^Datom datom]
                             (and (= movies-attr (.a datom))
                                  (is-documentary? (d/entity db (.v datom)))))]
    (fn [db ^Datom datom]
      (not (or (has-documentaries? db datom)
               (is-documentary? (d/entity db (.e datom))))))))

(defn find-id-paths [db source target]
  (let [filt (without-documentaries db)
        fdb (d/filter db filt)]
    (bidirectional-bfs source target (partial neighbors fdb))))

(defn find-annotated-paths
  [db source target]
  (let [ename (partial actor-or-movie-name db)
        annotate-node (fn [node]
                        (let [ent (d/entity db node)]
                          {:type (if (:person/name ent) "actor" "movie")
                           :year (:title/year ent)
                           :name (ename ent)
                           :entity ent}))]
    (->> (find-id-paths db source target)
         (map (partial mapv annotate-node)))))

(defn annotate-search [db search hard-mode]
  (let [[result1 result2] search
        paths (find-annotated-paths db (:actor-id result1) (:actor-id result2))
        paths (if hard-mode
                (filter ascending-years? paths)
                paths)
        total (count paths)
        bacon-number (int (/ (-> paths first count) 2))]
    {:total total
     :paths paths
     :start (:name result1)
     :end   (:name result2)
     :bacon-number bacon-number
     :hard-mode? hard-mode}))

(defn include-pairwise-connections [path]
  (-> []
      (into (mapcat (fn [x] [(first x) x]))
            (partition 2 1 path))
      (conj (peek path))))

(defn annotate-pairwise-path [db path]
  (mapv (fn [x]
          (if (coll? x)
            (not-empty
             (mapv (fn [{:title/keys [title year type genre]}]
                     (str title " (" year ") [" (some-> type :db/ident name) ": "
                          (str/join ", " (map (comp name :db/ident) genre))
                          "]"))
                   (apply collaborations db x)))
            (:person/name (d/pull db [:person/name] x))))
        path))

(comment

  (defonce conn (d/connect "datomic:dev://localhost:4334/imdb"))

  (let [start "Clark Gable"
        end  "Hugh Jackman"
        db (d/db conn)
        possibles (search db start end)
        [[{p1 :id} {p2 :id}] & _] possibles
        paths (bidirectional-bfs p1 p2 #(collaborators db %))]
    (for [path paths
          :let [p (include-pairwise-connections path)
                annotated (annotate-pairwise-path db p)]
          :when (every? some? annotated)]
      annotated))

  (let [db (d/db conn)
        p1 (:id (first (actor-search db "Hugh Jackman")))
        all-pairs (q '[:find ?p1 ?p2
                       :in $ ?p1
                       :where [?p1 :person/collaborators ?p2]]
                     db p1)
        keep-pairs (set (q '[:find ?p1 ?p2
                             :in $ % ?p1 [?p2 ...]
                             :where
                             [?p1 :person/roles ?m]
                             [?p2 :person/roles ?m]
                             (not (ignorable? ?m))]
                           db util/rules p1 (map peek all-pairs)))
        removed (remove keep-pairs all-pairs)]
    {:counts {:all (count all-pairs)
              :keep (count keep-pairs)
              :remove (count removed)}
     :keep keep-pairs
     :remove removed})

  (let [db (d/db conn)
        [[{p1 :id} {p2 :id}] & _] (search (d/db conn) "Hugh Jackman" "Anne Hathaway")]
    (q '[:find ?p2 .
         ;; ?p1 ?p2 (pull ?m1 [:db/id
         ;;                          :title/title
         ;;                          :title/year
         ;;                          {:title/type [:db/ident]}
         ;;                          {:title/genre [:db/ident]}]) #_[...]
         :in $ % ?p1 ?p2
         :where
         [?p1 :person/roles ?m1]
         [?p2 :person/roles ?m1]
         (not (ignorable? ?m1))]
       db util/rules p1 p2))

  (let [db (d/db conn)]
    (time
     (d/qseq
      {:query '[:find ?p1 ?p2
                :in $ % [[?p1 ?p2] ...]
                :where
                (not-join [?p1 ?p2]
                          [?p1 :person/roles ?m]
                          [?p2 :person/roles ?m]
                          (not (ignorable? ?m)))]
       :args [db util/rules (take 1000
                                  (map (juxt :e :v)
                                       (d/seek-datoms (d/db conn) :aevt :person/collaborators
                                                      17592202190000)))]
       :timeout 10000})))

;; Jay Leno

  (d/touch
   (d/entity (d/db conn) 17592201698067))

;; Anne Hathaway
  17592201705228

  (let [d (d/db conn)]
    (->> (d/seek-datoms d :aevt :person/collaborators 17592201898067)
         (map (juxt :e :v))
         (partition-all 10)
         (mapcat (fn [batch]
                   (->>
                    (d/query {:query '[:find ?p1 ?p2
                                       :in $ % [[?p1 ?p2] ...]
                                       :where
                                       (not-join [?p1 ?p2]
                                                 [?p1 :person/roles ?m]
                                                 [?p2 :person/roles ?m]
                                                 (not (ignorable? ?m)))]
                              :args [d util/rules batch]
                              :timeout 10000})
                    (map (fn [[p1 p2]] [:db/retract p1 :person/collaborators p2])))))
         (take 10)))

  :ok)
