(ns kevin.views.layout
  (:require [hiccup.page :refer [html5 include-css]]))

(defn common [& body]
  (html5
    [:head
     [:title "Welcome to kevin"]
     (include-css "/stylesheets/styles.css")]
     [:link {:href "http://fonts.googleapis.com/css?family=Old+Standard+TT:400italic"
             :rel "stylesheet" :type "text/css"}]
     [:link {:href "http://fonts.googleapis.com/css?family=Alegreya+Sans:800italic"
             :rel "stylesheet" :type "text/css"}]
     [:link {:href "http://fonts.googleapis.com/css?family=Dancing+Script"
             :rel "stylesheet" :type "text/css"}]
    [:body.results
     [:div#container
      [:h1 [:a "Kevin Bacon"]]
      [:h2 "find any old bacon number"]
      body]]))
