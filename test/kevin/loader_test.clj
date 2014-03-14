(ns kevin.loader-test
  (:require [clojure.test :refer :all]
            [kevin.loader :refer :all]))

(deftest extract-years-test
  (is (= (extract-year "Knocked Up (2007)")
         2007))
  (is (= (extract-year "Die Hard 2 (1990)")
         1990))
  (is (= (extract-year "2001: A Space Odyssey (1968)")
         1968))
  (is (= (extract-year "Closer (2004/I)")
         2004)))

