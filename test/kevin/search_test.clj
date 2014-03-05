(ns kevin.search-test
  (:require [clojure.test :refer :all]
            [kevin.search :refer :all]))

(deftest tracing-paths
  (testing "trace-paths"
    (is (= (trace-paths {:a nil} :a)
           [[:a]]))
    (is (= (trace-paths {:a #{:b}
                         :b nil} :a)
           [[:a :b]]))
    (is (= (trace-paths {:a #{:b :c}
                         :b nil
                         :c #{:b}} :a)
           [[:a :c :b] [:a :b]]))
    (is (= (trace-paths {:a #{:b}
                         :b #{:c :d}
                         :c #{:d}
                         :d nil} :a)
           [[:a :b :c :d] [:a :b :d]]))
    ))
