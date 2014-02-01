(ns kevin.system
  (:require [datomic.api :as d]))

(defn ensure-schema [conn]
  (or (-> conn d/db (d/entid :actor/name))
      @(d/transact conn (read-string (slurp "resources/schema.edn")))))

(defn ensure-db [db-uri]
  (let [newdb? (d/create-database db-uri)
        conn (d/connect db-uri)]
    (ensure-schema conn)
    conn))

(defn system
  "Returns a new instance of the whole application."
  []
  {:db {:uri "datomic:dev://localhost:4884/movies"}})

(defn start
  "Performs side effects to initialize the system, acquire resources,
  and start it running. Returns an updated instance of the system."
  [system]
  (let [db (:db system)
        conn (ensure-db (:uri db))]
    (assoc-in system [:db :conn] conn)))

(defn stop
  "Performs side effects to shut down the system and release its
  resources. Returns an updated instance of the system."
  [system]
  (when-let [conn (:conn (:db system))]
    (d/release conn))
  (assoc-in system [:db :conn] nil))
