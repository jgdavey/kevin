(ns kevin.search
  (:require [datomic.api :as d :refer [q]]
            [kevin.core :refer :all]))

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

(let [add (fnil conj #{})]
  (defn- add-reducer
    ([] {})
    ([map [key val]]
     (update-in map [key] add val))))

(defn- map-set-pairs [map pairs]
  (reduce add-reducer map pairs))

(defn paths
  "Returns a lazy seq of all non-looping path vectors starting with
  [<start-node>]"
  [nodes-fn path]
  (let [this-node (peek path)]
    (->> (nodes-fn this-node)
         (filter #(not-any? (fn [edge] (= edge [this-node %]))
                            (partition 2 1 path)))
         (mapcat #(paths nodes-fn (conj path %)))
         (cons path))))

(defn trace-paths [m start]
  (remove #(m (peek %)) (paths m [start])))

(defn- find-paths [from-map to-map matches]
  (set (mapcat (fn [n]
                 (let [froms (map reverse (trace-paths from-map n))
                       tos (map rest (trace-paths to-map n))]
                   (for [from froms
                         to tos]
                     (vec (concat from to))))) matches)))

(defn bidirectional-bfs [start end neighbors]
  (loop [preds {start nil}
         succs {end nil}
         q1 (list start)
         q2 (list end)
         iter 1]
    (when (and (seq q1) (seq q2) (< iter 11)) ; 5 "hops" or fewer
      (if (<= (count q1) (count q2))
        (let [pairs (for [node q1
                          nbr (neighbors node)
                          :when (not (contains? preds nbr))]
                      [nbr node])
              preds (map-set-pairs preds pairs)
              q1 (map first pairs)]
          (if-let [all (seq (filter #(contains? succs %) q1))]
            (find-paths preds succs all)
            (recur preds succs q1 q2 (inc iter))))

        (let [pairs (for [node q2
                          nbr (neighbors node)
                          :when (not (contains? succs nbr))]
                      [nbr node])
              succs (map-set-pairs succs pairs)
              q2 (map first pairs)]
          (if-let [all (seq (filter #(contains? preds %) q2))]
            (find-paths preds succs all)
            (recur preds succs q1 q2 (inc iter))))))))

(defn path-at-depth [db source target depth]
  (let [rule (symbol (str "acted-with-" depth))]
    (q (concat '[:find ?path
                 :in $ % ?actor ?target
                 :where]
               [(list rule '?actor '?target '?path)])
      db acted-with-rules source target)))

(defn find-id-paths [db source target]
  (bidirectional-bfs source target (partial neighbors db)))

(defn find-annotated-paths
  [db source target & {:keys [limit] :or {limit 1000}}]
  (let [ename (partial actor-or-movie-name db)
        annotate-node (fn [node]
                        (let [ent (d/entity db node)]
                          {:type (if (:actor/name ent) "actor" "movie")
                           :name (ename ent)
                           :entity ent}))]
    (->> (find-id-paths db source target)
         (map (partial mapv annotate-node))
         (take limit))))
