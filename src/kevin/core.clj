(ns kevin.core
  (:require [datomic.api :as d :refer [q db]]
            [clojure.string :refer [split join] :as str]
            [clojure.zip :as zip]
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
     [?e1 :actor/movies ?m]
     [?e2 :actor/movies ?m]
     [(!= ?e1 ?e2)]
     [(vector ?e1 ?m) ?path]]
    [(acted-with-1 ?e1 ?e2 ?path)
     (acted-with ?e1 ?e2 ?path)]
    [(acted-with-2 ?e1 ?e2 ?path)
     (acted-with ?e1 ?x ?p1)
     (acted-with ?x ?e2 ?p2)
     [(concat ?p1 ?p2) ?path]]
    [(acted-with-3 ?e1 ?e2 ?path)
     (acted-with-2 ?e1 ?x ?p1)
     (acted-with ?x ?e2 ?p2)
     [(concat ?p1 ?p2) ?path]]
    [(acted-with-4 ?e1 ?e2 ?path)
     (acted-with-3 ?e1 ?x ?p1)
     (acted-with ?x ?e2 ?p2)
     [(concat ?p1 ?p2) ?path]]])

(defn format-query
  "Makes each word of query required, front-stemmed

  (format-query \"Foo bar\")
   ;=> \"+Foo* +bar*\"

  This maps to Lucene's QueryParser.parse
  See http://lucene.apache.org/core/3_6_1/api/core/org/apache/lucene/queryParser/QueryParser.html"
  [query]
  (->> (split query #",?\s+")
       (remove str/blank?)
       (map #(str "+" % "*"))
       (join " ")))

(defn actor-search
  "query will be passed as-is to Lucene"
  [db query]
  (if (str/blank? query)
    #{}
    (q '[:find ?e ?name
        :in $ ?search
        :where [(fulltext $ :person/name ?search) [[?e ?name]]]]
      db query)))

(defn actor-or-movie-name [db eid]
  (let [ent (d/entity db (e eid))]
    (or (:movie/title ent) (:person/name ent))))

(defn referring-to
  "Find all entities referring to an eid as a certain attribute."
  [db eid]
   (->> (d/datoms db :vaet (e eid))
        (map :e)))

(defn eids-with-attr-val
  "Return eids with a given attribute and value."
  [db attr val]
  (->> (d/datoms db :avet attr val)
       (map :e)))

(defn actor-name->eid
  "db is database value
  name is the actor's name"
  [db name]
  (-> (eids-with-attr-val db :person/name name)
    first))

(defn eid->actor-name
  "db is database value
  name is the actor's name"
  [db eid]
  (-> (d/entity db (e eid))
      :person/name))

(defn actor-movies
  [db eid]
  (map :v (d/datoms db :eavt eid :actor/movies)))

(defn immediate-connections
  "d is database value
  eid is actor's entity id"
  [db eid]
  (->> (actor-movies db eid)
      (mapcat (partial referring-to db))))

(defn neighbors
  "d is database value
  eid is an actor or movie eid"
  [db eid]
  (or (seq (actor-movies db (e eid)))
      (seq (referring-to db (e eid)))))

(defn zipper
  "db is database value
  eid is actor's entity id"
  [db eid]
  (let [children (partial immediate-connections db)
        branch? (comp seq children)
        make-node (fn [_ c] c)]
    (zip/zipper branch? children make-node eid)))

(defn name-or-search [db person]
  (let [actor (actor-name->eid db person)
        result {:name person :actor-id actor}]
    (assoc result :names
           (cond
             (not person) (list)
             actor (list person)
             :else (->> person
                        format-query
                        (actor-search db)
                        (map last))))))

(defn search [db & people]
  (mapv (partial name-or-search db) people))

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
  (let [genres (:movie/genre entity)]
    (and genres (contains? genres :movie.genre/documentary))))

(defn without-documentaries
  "Returns a function suitable for use with datomic.api/filter"
  [db]
  (let [movies-attr (d/entid db :actor/movies)
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
                           :year (:movie/year ent)
                           :name (ename ent)
                           :entity ent}))]
    (->> (find-id-paths db source target)
         (map (partial mapv annotate-node)))))
