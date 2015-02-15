(ns user
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer (pprint)]
            [clojure.repl :refer :all]
            [clojure.zip :as zip]
            [clojure.tools.namespace.repl :refer (refresh refresh-all)]
            [ring.server.standalone :refer (serve)]
            [datomic.api :as d :refer (db q)]
            [kevin.system :as sys]
            [kevin.expunge]
            [kevin.core :refer :all]
            [kevin.search :refer :all]))

(defonce system nil)

(defn start-server [system]
  (let [server (serve (get-in system [:web :handler]) (:web system))]
    (assoc-in system [:web :server] server)))

(defn stop-server [system]
  (when-let [server (get-in system [:web :server])]
    (.stop server)
    (assoc-in system [:web :server] nil)))

(defn init
  "Constructs the current development system."
  []
  (alter-var-root #'system (constantly (sys/system))))

(defn start
  "Starts the current development system."
  []
  (alter-var-root #'system sys/start)
  (alter-var-root #'system start-server))

(defn stop
  "Shuts down and destroys the current development system."
  []
  (when system
    (alter-var-root #'system stop-server)
    (alter-var-root #'system sys/stop)))

(defn go
  "Initializes the current development system and starts it running."
  []
  (init)
  (start))

(defn reset []
  (stop)
  (refresh :after 'user/go))

(defn import-sample-data
  "Transacts the sample data from `resources/sample.edn` into current
  system's database connection. Assumes top-level system var has an active
  database connection."
  []
  { :pre (:conn (:db system)) }
  (let [conn (-> system :db :conn)
        actors (read-string (slurp "resources/sample.edn"))
        tx-fn (fn [[name movies]]
                {:db/id (d/tempid :db.part/user)
                :person/name name
                :actor/movies (mapv (fn [m] {:db/id (d/tempid :db.part/user)
                                             :movie/title m}) movies)})]

    @(d/transact conn (map tx-fn actors))
    (kevin.loader/add-years-to-movies conn)
    (d/request-index conn)
    :ok))

(comment

(reset)

;; movies an actor was in
(q '[:find [?mn ...]
     :in $ ?n
     :where
     [?e :person/name ?n]
     [?e :actor/movies ?m]
     [?m :movie/title ?mn]]
   (-> system :db :conn db)
   "Bacon, Kevin (I)")

;; movies an actor with name like query was in
(q '[:find [(pull ?e [:person/name {:actor/movies [:movie/title]}]) ...]
     :in $ ?q
     :where
     [(fulltext $ :person/name ?q) [[?e ?name]]]]
   (-> system :db :conn db)
   "+Bacon +Kevin")

;; number of movies, total
(time (q '[:find (count ?e) :where [?e :movie/title]]
          (-> system :db :conn db)))

;; number of movies with actors
(time
  (let [d (-> system :db :conn db)]
    (q '[:find (count ?e)
          :where
          [?e :movie/title]
          [_ :actor/movies ?e]]
        d)))

;; number of movies with no actors
(time
  (let [d (-> system :db :conn db)
        movies (q '[:find ?e :where [?e :movie/title]] d)]
    (->> (map (fn [[id]] (d/entity d id)) movies)
          (remove (fn [e] (:actor/_movies e)))
          count)))

;; retract video games
(let [d (-> system :db :conn db)]
  (->> (q '[:find ?e ?name
            :where
            [?e :movie/title ?name]] d)
        (filter (fn [[e n]] (not= -1 (.indexOf n "(VG)"))))
        (mapv (fn [[e _]] [:db.fn/retractEntity e]))
        (d/transact (-> system :db :conn))
        (deref)
        ))

;; zipper
(let [d (-> system :db :conn db)
      a (d/entid d [:person/name "Barth, Clayton"])
      actor-name (partial eid->actor-name d)
      kevin (d/entid d [:person/name "Bacon, Kevin (I)"])
      tree (zipper d a)]
  (time (some (fn [n] (when (= kevin n) n)) tree)))


;; bi-directional bfs
(def from-bfs
  (let [d (-> system :db :conn db)
        clay (d/entid d [:person/name "Barth, Clayton"])
        kevin (d/entid d [:person/name "Bacon, Kevin (I)"])
        neighbor-fn (partial neighbors d)]
    (time (bidirectional-bfs clay kevin neighbor-fn)))
  )

;; queue-based search
(let [d (-> system :db :conn db)
      clay (d/entid d [:person/name "Barth, Clayton"])
      kevin (d/entid d [:person/name "Bacon, Kevin (I)"])
      neighbor-fn (partial neighbors d)
      actor-name (partial actor-or-movie-name d)]
  (time (map actor-name (bfs clay kevin neighbor-fn))))

;; history-graham?
(time
(let [d (-> system :db :conn db)
      d (d/filter d (without-documentaries d))
      kevin (d/entid d [:person/name "Bacon, Kevin (I)"])
      neighbor-fn (partial neighbors d)]
  (->> (degrees-of-separation kevin neighbor-fn :up-to 15)
       (reduce-kv (fn [m k v]
                    (if (even? k)
                      (assoc m (/ k 2) (count v))
                      m)) {})))
)

;; query engine search (3 degrees)
(let [d (-> system :db :conn db)
      clay (actor-name->eid d "Barth, Clayton")
      kevin (actor-name->eid d "Bacon, Kevin (I)")
      actor-name (partial eid->actor-name d)]
  (time (q '[:find ?actor ?m1 ?target
                          :in $ % ?actor ?target
                          :where (acted-with ?actor ?m1 _)
                          (acted-with ?m1 ?target _)]
                        d acted-with-rules clay kevin)))

;; query engine search (4 degrees)
(let [d (-> system :db :conn db)
      clay (actor-name->eid d "Barth, Clayton")
      kevin (actor-name->eid d "Bacon, Kevin (I)")
      actor-name (partial eid->actor-name d)]
  (time (map (partial map actor-name)
              (q '[:find ?actor ?m1 ?m2 ?target
                   :in $ % ?actor ?target
                   :where (acted-with ?actor ?m1 _)
                   (acted-with ?m1 ?m2 _)
                   (acted-with ?m2 ?target _)]
                 d acted-with-rules clay kevin))))

;; using path from rule
(let [d (-> system :db :conn db)
      clay (d/entid d [:person/name "Barth, Clayton"])
      kevin (d/entid d [:person/name "Bacon, Kevin (I)"])
      ename  (partial actor-or-movie-name d)]
  (time (map first
             (q '[:find ?path
                  :in $ % ?actor ?target
                  :where
                  (acted-with-3 ?actor ?target ?path)]
                d acted-with-rules clay kevin))))

;; export movies and actors
(let [d (-> system :db :conn db)
      reducer (fn [map [k v]] (assoc map k (conj (get map k []) v)))
      actor-map (->> (kevin.expunge/actor-names "data/movies-small.list.gz" d)
                    (reduce reducer {}))]
    (spit "resources/sample.edn" (with-out-str (pr actor-map))))


;; quick n dirty profiling
(let [d (-> system :db :conn db)
      clay (d/entid d [:person/name "Barth, Clayton"])
      kevin (d/entid d [:person/name "Bacon, Kevin (I)"])
      neighbor-fn (partial neighbors d)]
  (time (dotimes [_ 50]
          (bidirectional-bfs clay kevin neighbor-fn))))


;; Filter out documentaries
(let [d (-> system :db :conn db)
      fd (d/filter d (without-documentaries d))]
  (q '[:find ?m ?t ?g
       :in $ ?t
       :where [?m :movie/title ?t]
       [?m :movie/genre ?g]] d "Going to Pieces: The Rise and Fall of the Slasher Film (2006)"))

;; Genres and counts

(def genre-counts (let [d (-> system :db :conn db)]
                    (q '[:find ?g (count ?e)
                         :in $
                         :where
                         [?e :movie/genre ?gid]
                         [?gid :db/ident ?g]] d)))

(pprint (map (fn [[g c]] [(name g) c]) (sort-by peek genre-counts)))

(d/touch (d/entity (-> system :db :conn db) 17592191235171))

(deref (d/transact (-> system :db :conn) [[:db/add 17592191235171 :movie/genre 0]]))

(d/q '[:find ?e ?a ?v
       :in ?log ?t1 ?t2
       :where [(tx-ids ?log ?t1 ?t2) [?tx ...]]
              [(tx-data ?log ?tx) [[?e ?a ?v]]]]
     (d/log (-> system :db :conn)) #inst "2014-11-11" #inst "2014-11-12")

)
