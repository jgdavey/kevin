(ns kevin.views
  (:require [hiccup.util :refer [url]]
            [clojure.string :as str]
            [kevin.util :refer :all]
            [net.cgrand.enlive-html :as html
             :refer [defsnippet deftemplate do-> content clone-for
                     substitute set-attr nth-of-type first-of-type
                     last-of-type]]))

(defn- form-input [nth]
  [[:fieldset (nth-of-type 1)] :dl [:dd (nth-of-type nth)] :input])

(defn emit [form]
  (if (some map? form)
    (html/emit* form)
    form))

(defn render [& args]
  (apply str (flatten (map emit args))))

(deftemplate main-template "templates/index.html"
  [& {:keys [body title]}]
  [:head :title] (content (str/join " - "
                                    (filter identity [title "Kevin Bacon"])))
  [:#container :> :h1 :a] (set-attr :href "/")
  [:#container :ul] (substitute body))

(defsnippet form "templates/search_form.html" [:#container :form]
  [person1 person2 & args]
  [:form] (do->
            (set-attr :action "search")
            (set-attr :method "GET"))
  (form-input 1) (do->
                   (set-attr :name "person1")
                   (set-attr :value person1))
  (form-input 2) (do->
                   (set-attr :name "person2")
                   (set-attr :value person2)))

(defsnippet possibility "templates/disambiguate.html" [:#disambiguate [:li (nth-of-type 1)]]
  [[person1 person2]]
  [:a] (set-attr :href (str (url "/search" {:person1 person1 :person2 person2})))
  [:a :> html/text-node] (html/wrap :rel)
  [:a :> [:rel html/first-child]] (content (format-name person1))
  [:a :> [:rel html/last-child]] (content (format-name person2))
  [:a :> :rel] html/unwrap)

(defsnippet possibilities "templates/disambiguate.html" [:#disambiguate]
  [pairs]
  [:ul] (content (map possibility pairs)))

(def ^:dynamic *result-sel* [:.result_list :> [:ul (nth-of-type 1)] :> [:li (nth-of-type 1)]])

(defn simple-escape [text]
  (.. text (replace " " "+") (replace "&" "%26")))

(defn- imdb-link [{:keys [name type]}]
  (str "http://imdb.com/find?exact="
       (= type "movie")
       "&q="
       (simple-escape name)))

(defn- display-name [{:keys [name type]}]
  (if (= type "actor")
    (format-name name)
    name))

(defsnippet result-node "templates/results.html"  (conj *result-sel* [:li (nth-of-type 1)])
  [node]
  [:li] (set-attr :class (:type node))
  [:li :a] (do->
             (set-attr :href (imdb-link node))
             (content (display-name node))))

(defsnippet result "templates/results.html" *result-sel*
  [path]
  [:ul] (content (map result-node path)))

(defsnippet results "templates/results.html" [:#results]
  [{:keys [paths total start end bacon-number]}]
  [:.result_list :> [:ul (html/but first-of-type)]] nil
  [:.result_list :> [:ul first-of-type]] (clone-for [path paths]
                                                    (content (result path)))
  [:.result_list :> :h3] (content (str total " paths"))
  [:.bacon_number :mark] (content (str bacon-number))
  [:.bacon_number [:p first-of-type]] (content (format-name start))
  [:.bacon_number [:p last-of-type]] (content (format-name end)))

(defn disambiguate [search {:keys [hard-mode]}]
  (let [[result1 result2] search
        pairs (for [person1 (:names result1)
                    person2 (:names result2)]
                [person1 person2])]
    (main-template :title "Did you mean one of these?"
                   :body [(possibilities pairs)
                          (html/html [:h2 "Try a new search"])
                          (form (:name result1) (:name result2) hard-mode)])))

(defn results-page [{:keys [paths start end] :as res}]
  (main-template :body (if (seq paths)
                         (results res)
                         "Not linkable in 5 hops or fewer")
                 :title (str "From " start " to " end)))
