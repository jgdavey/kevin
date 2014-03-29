(ns kevin.search)

(defn bfs
  "bread-first search, one-directional"
  [start end neighbor-fn]
  (let [queue (conj clojure.lang.PersistentQueue/EMPTY [start])
        visited #{start}
        end-neighbors (neighbor-fn end)
        found? (fn [n] (some #{n} end-neighbors))]
    (loop [q queue
           v visited
           i 0]
      (when (seq q)
        (let [path (peek q)
              node (last path)]
          (if (found? node)
            (do
              (println "Finished in " i " iterations")
              (conj path end))
            (let [neighbors (remove v (neighbor-fn node))
                  paths (map (partial conj path) neighbors)]
              (recur (into (pop q) paths) (into v neighbors) (inc i)))))))))

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

(defn degrees-of-separation
  [start neighbor-fn & {:keys [up-to] :or {up-to 13}}]
  (loop [q #{start}
         distances {0 q}
         i 1]
    (if (< i up-to) ; Has anyone really been far even?
      (let [visited (vals distances)
            next-q (set (flatten (for [node q
                                       neighbor (neighbor-fn node)
                                       :when (not-any? #(contains? % neighbor) visited)]
                                   neighbor)))]
        (recur next-q (assoc distances i next-q) (inc i)))
      distances)))

(defn trace-paths [m start]
  (remove #(m (peek %)) (paths m [start])))

(defn- find-paths [from-map to-map matches]
  (for [n matches
        from (map reverse (trace-paths from-map n))
        to (map rest (trace-paths to-map n))]
    (vec (concat from to))))

(defn- neighbor-pairs [neighbors q coll]
  (for [node q
        nbr (neighbors node)
        :when (not (contains? coll nbr))]
    [nbr node]))

(defn bidirectional-bfs [start end neighbors]
  (let [find-pairs (partial neighbor-pairs neighbors)
        overlaps (fn [coll q] (seq (filter #(contains? coll %) q)))
        map-set-pairs (fn [map pairs]
                        (persistent! (reduce (fn [map [key val]]
                                  (assoc! map key (conj (get map key #{}) val)))
                                (transient map) pairs)))]
    (loop [preds {start nil} ; map of outgoing nodes to where they came from
           succs {end nil}   ; map of incoming nodes to where they came from
           q1 (list start)   ; queue of outgoing things to check
           q2 (list end)     ; queue of incoming things to check
           iter 1]
      (when (and (seq q1) (seq q2) (< iter 13)) ; 6 "hops" or fewer
        (if (<= (count q1) (count q2))
          (let [pairs (find-pairs q1 preds)
                preds (map-set-pairs preds pairs)
                q1 (map first pairs)]
            (if-let [all (overlaps succs q1)]
              (find-paths preds succs (set all))
              (recur preds succs q1 q2 (inc iter))))
          (let [pairs (find-pairs q2 succs)
                succs (map-set-pairs succs pairs)
                q2 (map first pairs)]
            (if-let [all (overlaps preds q2)]
              (find-paths preds succs (set all))
              (recur preds succs q1 q2 (inc iter)))))))))
