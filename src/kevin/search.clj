(ns kevin.search)

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
