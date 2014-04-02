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
  [:#container :> #{:h1 :h2} :a] (set-attr :href "/")
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
  [:a] (set-attr :href (str (url "/search" {:person1 (:name person1) :person2 (:name person2)})))
  [:a :> html/text-node] (html/wrap :rel)
  [:a :> [:rel html/first-child]] (content (format-name (:name person1)))
  [:a :> [:rel html/last-child]] (content (format-name (:name person2)))
  [:a :> :rel] html/unwrap)

(defsnippet possibilities "templates/disambiguate.html" [:#disambiguate]
  [pairs]
  [:ul] (content (map possibility pairs)))

(def ^:dynamic *result-sel* [:.result_list :> [:ul (nth-of-type 1)] :> [:li (nth-of-type 1)]])

(defn simple-escape [^String text]
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

(defn degrees-description [bacon-number]
  (let [degrees (str bacon-number " degree" (when-not (= 1 bacon-number) "s"))]
    (str "(with " degrees " of separation)")))

(defsnippet results "templates/results.html" [:#results]
  [{:keys [paths total start end bacon-number]}]
  [:.result_list :> [:ul (html/but first-of-type)]] nil
  [:.result_list :> [:ul first-of-type]] (clone-for [path paths]
                                                    (content (result path)))
  [:.result_list :> :h3] (content (str (count paths) " path" (when-not (= 1 (count paths)) "s")))
  [:.result_list :> :h4] (content (degrees-description bacon-number))
  [:.bacon_number :mark] (content (str bacon-number))
  [:.bacon_number [:p first-of-type]] (content (format-name start))
  [:.bacon_number [:p last-of-type]] (content (format-name end)))

(defn disambiguate [pairs {:keys [hard-mode person1 person2]}]
  (main-template :title "Did you mean one of these?"
                 :body [(if (seq pairs)
                          (possibilities pairs)
                          (html/html [:h2 "Oops. We couldn't find one of those actors"]))
                        (html/html [:h2 "Try a new search"])
                        (form person1 person2 hard-mode)]))

(defn results-page [{:keys [paths start end] :as res}]
  (main-template :body (if (seq paths)
                         (results res)
                         "Not linkable in 5 hops or fewer")
                 :title (str "From " start " to " end)))
