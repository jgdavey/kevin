(ns kevin.routes.home
  (:require [compojure.core :refer :all]
            [datomic.api :as d]
            [noir.util.cache :refer [cache!]]
            [kevin.core :as s]
            [kevin.views :as views]))

(noir.util.cache/set-timeout! 10)
(noir.util.cache/set-size! 500)

(defn home []
  (cache! :home
    (views/main-template
      :body (views/form "Kevin Bacon" nil nil))))

(defn- cache-key [search]
  (mapv :actor-id (first search)))

(defn search [context {:keys [person1 person2 hard-mode] :as params}]
  (let [db (-> context :db :conn d/db)
        search (s/search db person1 person2)]
    (if (= 1 (count search))
      (cache! (cache-key search)
        (views/results-page (s/annotate-search db (first search) (seq hard-mode))))
      (views/disambiguate search params))))

(defn home-routes [context]
  (routes
    (HEAD "/" [] "") ;; heartbeat response
    (GET "/" [] (home))
    (GET "/search" {params :params} (search context params))))
