(ns kevin.util-test
  (:require [clojure.test :refer [deftest is are]]
            [kevin.util :refer [format-query format-name]]))

(deftest test-format-query
  (is (= "+J* +Digg" (format-query "J* Digg")))
  (is (= "+J +Digg +\\(I\\)" (format-query "J Digg (I)")))
  (is (= "+Anya +\"Taylor\\-Joy\"" (format-query "Anya Taylor-Joy")))
  (is (= "+Samuel +L +Jackson" (format-query "Samuel L. Jackson"))))

(deftest test-format-name
  (are [x y] (= (format-name x) y)
       "Wayne, John"     "John Wayne"
       "Wayne, John (I)" "John Wayne (I)"
       "Shakira"         "Shakira"
       "Watts, J (I) W"  "J (I) W Watts"
       "Wayne John, Juan Wayne (XII)" "Juan Wayne Wayne John (XII)"))
