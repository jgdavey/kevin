(ns kevin.expunge-test
  (:require [clojure.test :refer :all]
            [kevin.expunge :refer :all]))

(deftest parsing
  (is (= ["Scanner Darkly, A (2006)"
          "Mad Dog and Glory (1993)"]
         (parse-movie-titles ["2003 MTV Movie Awards (2003) (TV)	2003"
                              "Comic Books & Superheroes (2001) (V)	2001"
                              "Scanner Darkly, A (2006)	2006"
                              "Mad Dog and Glory (1993)	1993"]))))

