(ns kevin.system
  (:require [datomic.api :as d]
            [kevin.handler :as handler]
            [environ.core :refer [env]]
            [ring.server.standalone :refer (serve)]))

(defn- ensure-schema [conn]
  (or (-> conn d/db (d/entid :person/name))
      @(d/transact conn (read-string (slurp "resources/schema.edn"))))
  (or (-> conn d/db (d/entid :movie/genre))
      @(d/transact conn (read-string (slurp "resources/genres.edn")))))

(defn- ensure-db [db-uri]
  (let [newdb? (d/create-database db-uri)
        conn (d/connect db-uri)]
    (ensure-schema conn)
    conn))

(defn start-db [system]
  (let [db (:db system)
        conn (ensure-db (:uri db))]
    (assoc-in system [:db :conn] conn)))

(defn- stop-db [system]
  (when-let [conn (:conn (:db system))]
    (d/release conn))
  (assoc-in system [:db :conn] nil))

(defn- setup-handler [system]
  (let [web-opts (:web system)
        handler (handler/app system)]
    (-> system
        (assoc-in [:web :handler] handler))))

(defn- teardown-handler [system]
  (-> system
      (assoc-in [:web :handler] nil)))

(defn system
  "Returns a new instance of the whole application."
  []
  {:db {:uri (env :datomic-db-url)}
   :web {:open-browser? false}})

(defn start
  "Performs side effects to initialize the system, acquire resources,
  and start it running. Returns an updated instance of the system."
  [system]
   (-> system
       start-db
       setup-handler))

(defn stop
  "Performs side effects to shut down the system and release its
  resources. Returns an updated instance of the system."
  [system]
  (when system
    (-> system
        teardown-handler
        stop-db)))

;; external ring handlers
(defonce sys nil)
(defonce handler nil)

(defn destroy []
  (let [s (stop sys)]
    (alter-var-root #'sys nil)
    (alter-var-root #'handler nil)))

(defn init []
  (let [s (start (system))]
    (alter-var-root #'sys (constantly s))
    (alter-var-root #'handler (constantly (get-in s [:web :handler])))
    s))
