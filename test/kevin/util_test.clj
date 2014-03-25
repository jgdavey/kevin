(ns kevin.util-test
  (:use [clojure.test]
        [kevin.util]))

(deftest test-format-query
  (is (= "+J* +Digg*" (format-query "J Digg"))))
