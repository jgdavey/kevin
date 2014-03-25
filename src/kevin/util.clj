(ns kevin.util
  (:require [clojure.string :refer [split join] :as str]))

(defn format-query
  "Makes each word of query required, front-stemmed

  (format-query \"Foo bar\")
   ;=> \"+Foo* +bar*\"

  This maps to Lucene's QueryParser.parse
  See http://lucene.apache.org/core/3_6_1/api/core/org/apache/lucene/queryParser/QueryParser.html"
  [query]
  (->> (split query #",?\s+")
       (remove str/blank?)
       (map #(str "+" % "*"))
       (join " ")))
