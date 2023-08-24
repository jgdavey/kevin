(ns kevin.util
  (:require [clojure.string :refer [split join] :as str])
  (:import [com.datomic.lucene.queryParser QueryParser]))

(defn- tokenize-query [q]
  (let [wild? (str/ends-with? q "*")
        escaped (-> q
                    (str/replace #"(\.|\*)" "")
                    (QueryParser/escape))]
    (cond
      (str/includes? escaped "-") (str "+\"" escaped "\"")
      wild? (str "+" escaped "*")
      :else (str "+" escaped))))

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

(def rules
  '[[(ignorable? [?m])
     (or
      [?m :title/type :title.type/tv.special]
      [?m :title/type :title.type/video]
      [?m :title/genre :title.genre/short]
      [?m :title/genre :title.genre/music]
      [?m :title/genre :title.genre/documentary]
      [?m :title/genre :title.genre/talk-show]
      [?m :title/genre :title.genre/news])]
    [(non-ignorable? [?m])
     (not (ignorable? ?m))]])
