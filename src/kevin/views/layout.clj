(ns kevin.views.layout
  (:require [hiccup.page :refer [html5 include-css]]))

(defn common [& body]
  (html5
    [:head
     [:title "Welcome to kevin"]
     (include-css "/css/screen.css")]
    [:body
     [:div#container
      [:h1 "Kevin Bacon"]
      body]]))
