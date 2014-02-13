(ns kevin.handler
  (:require [compojure.core :refer [defroutes routes]]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.middleware.file-info :refer [wrap-file-info]]
            [hiccup.middleware :refer [wrap-base-url]]
            [compojure.handler :as handler]
            [compojure.route :as route]
            [kevin.routes.home :refer [home-routes]]))

(defroutes app-routes
  (route/resources "/")
  (route/not-found "Not Found"))

(defn app [context]
  (-> (home-routes context)
      (routes app-routes)
      (handler/site)
      (wrap-base-url)))

