(ns kevin.core-test
  (:use [clojure.test]
        [kevin.core]))

(deftest ascending-years
  (is (ascending-years? [{:year 2010} {:year 2011}]))
  (is (ascending-years? [{:year 2010} {:year nil} {:year 2011}]))
  (is (ascending-years? [{:year nil}]))
  (is (ascending-years? [{:year 2012} {:year 2012}]))
  (is (ascending-years? [{:year 2010} {:year 2011} {:year 2011}]))
  (is (not (ascending-years? [{:year 2012} {:year 2011}])))
  (is (not (ascending-years? [{:year 2011} {:year 2011} {:year nil} {:year 2010}]))))
