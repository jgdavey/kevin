(ns kevin.routes.home
  (:require [compojure.core :refer :all]
            [datomic.api :as d]
            [kevin.core :as s]
            [kevin.views :as views]))

(defn home []
  (views/main-template
    :body (views/form "Bacon, Kevin (I)" nil nil)))

(defn search [context {:keys [person1 person2 hard-mode] :as params}]
  (let [db (-> context :db :conn d/db)
        search (s/search db person1 person2)]
    (if (= 1 (count search))
      (views/results-page (s/annotate-search db (first search) (seq hard-mode)))
      (views/disambiguate search params))))

(defn home-routes [context]
  (routes
    (HEAD "/" [] "") ;; heartbeat response
    (GET "/" [] (home))
    (GET "/search" {params :params} (search context params))))
