(ns kevin.routes.home
  (:require [compojure.core :refer :all]
            [hiccup.form :refer :all]
            [hiccup.util :refer [url]]
            [hiccup.element :refer [link-to]]
            [clojure.string :as str]
            [datomic.api :as d]
            [kevin.core :as s]
            [kevin.views.layout :as layout]))

(defn form [person1 person2]
  (form-to [:get "search"]
           [:h2 "Search"]
           [:fieldset
            [:p "Choose two people"]
            [:p
             (label "person1" "From")
             (text-field "person1" person1)]
            [:p
             (label "person2" "To")
             (text-field "person2" person2)]]
           [:fieldset.actions
            (submit-button "Go")]))

(defn home []
  (layout/common
    (form nil "Bacon, Kevin (I)")))

(defn results [db search]
  (let [[result1 result2] search
        paths (s/find-annotated-paths db (:actor-id result1) (:actor-id result2) :limit 50)
        bacon-number (int (/ (-> paths first count) 2))]
    (layout/common
      (if (seq paths)
        [:div#results
         [:h2 "Results"]
         [:p "Bacon Number: " [:strong bacon-number]]
         (for [path paths]
           [:ul
            (for [node path] [:li {:class (:type node)} (:name node)])])]
        [:p "Not linkable in 5 hops or fewer"]))))

(defn disambiguate [search]
  (let [[result1 result2] search]
    (layout/common
      [:h2 "Did you mean one of the below?"]
      (or (seq (for [person1 (:names result1)
                     person2 (:names result2)]
                 [:p (link-to
                       (url "/search" {:person1 person1 :person2 person2})
                       (str person1 " -> " person2))]))
          [:p "No Results. Try another search"])
      (form (:name result1) (:name result2)))))

(defn search [context {:keys [person1 person2]}]
  (let [db (-> context :db :conn d/db)
        search (s/search db person1 person2)]
    (if (every? :actor-id search)
      (results db search)
      (disambiguate search))))

(defn home-routes [context]
  (routes
    (HEAD "/" [] "") ;; heartbeat response
    (GET "/" [] (home))
    (GET "/search" {params :params} (search context params))))
