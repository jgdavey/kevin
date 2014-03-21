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
           [:fieldset
            [:dl
             [:dt (label "person1" "Choose an actor")]
             [:dd (text-field "person1" person1)]
             [:dt (label "person2" "Choose another actor")]
             [:dd (text-field "person2" person2)]]]
           [:fieldset.actions
            (submit-button "calculate")]))

(defn home []
  (layout/common
    (form nil "Bacon, Kevin (I)")))

(defn simple-escape [text]
  (.. text (replace " " "+") (replace "&" "%26")))

(defn- imdb-link [name type]
  [:a {:href (str "http://imdb.com/find?exact="
                  (= type "movie")
                  "&q="
                  (simple-escape name))}
   name])

(defn results [db search]
  (let [[result1 result2] search
        paths (s/find-annotated-paths db (:actor-id result1) (:actor-id result2))
        bacon-number (int (/ (-> paths first count) 2))]
    (layout/common
      (if (seq (take 50 paths))
        [:div#results
         [:div.bacon_number
          [:p (:name result1)]
          [:mark bacon-number]
          [:p (:name result2)]]
         [:div.result_list
          [:ul
           (for [path paths]
             [:li
              [:ul
               (for [node path] [:li {:class (:type node)}
                                 (imdb-link (:name node) (:type node))])]])]]]
        [:p "Not linkable in 5 hops or fewer"]))))

(defn disambiguate [search]
  (let [[result1 result2] search]
    (layout/common
      [:div#disambiguate
       [:h2 "Did you mean one of the below?"]
       (or (seq (for [person1 (:names result1)
                      person2 (:names result2)]
                  [:p (link-to
                        (url "/search" {:person1 person1 :person2 person2})
                        (str person1 " &rarr; " person2))]))
           [:p "No Results. Try another search"])
       [:h2 "Try a new search, if you like"]
       (form (:name result1) (:name result2))])))

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
