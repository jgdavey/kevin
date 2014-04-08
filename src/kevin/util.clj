(ns kevin.util
  (:require [clojure.string :refer [split join] :as str])
  (:import [com.datomic.lucene.queryParser QueryParser]))

(defn- tokenize-query [q]
  (let [escaped (QueryParser/escape q)]
    (if (= q escaped)
      (str "+" escaped "*")
      (str "+" escaped))))

(defn format-query
  "Makes each word of query required, front-stemmed.
   Escapes all special characters.

  (format-query \"Foo bar\")
   ;=> \"+Foo* +bar*\"

  This maps to Lucene's QueryParser.parse
  See http://lucene.apache.org/core/3_6_1/api/core/org/apache/lucene/queryParser/QueryParser.html"
  [query]
  (->> (split query #",?\s+")
       (remove str/blank?)
       (map tokenize-query)
       (join " ")))

(defn format-name [name]
  (str/replace-first name #"^([^,]+), (.+?)( \([IVX]+\))?$" "$2 $1$3"))
