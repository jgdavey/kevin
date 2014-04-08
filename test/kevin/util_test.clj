(ns kevin.util-test
  (:use [clojure.test]
        [kevin.util]))

(deftest test-format-query
  (is (= "+J* +Digg*" (format-query "J Digg")))
  (is (= "+J* +Digg* +\\(I\\)" (format-query "J Digg (I)"))))

(deftest test-format-name
  (are [x y] (= (format-name x) y)
       "Wayne, John"     "John Wayne"
       "Wayne, John (I)" "John Wayne (I)"
       "Shakira"         "Shakira"
       "Watts, J (I) W"  "J (I) W Watts"
       "Wayne John, Juan Wayne (XII)" "Juan Wayne Wayne John (XII)"))
