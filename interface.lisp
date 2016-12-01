
(defgeneric calculate-distance (from-node to-node)
  (:documentation "Calculate the distance between two peers. If the metric function is symmetric, the from-node
and to-node are interchangable."))
