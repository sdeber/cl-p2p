(asdf:defsystem cl-p2p
  :name "cl-p2p"
  :maintainer "Lin Li"
  :author "Lin Li"
  :description "P2P library in Common Lisp"
  :depends-on (:bordeaux-threads :usocket :ironclad)
  :components
  ((:module "src"
            :serial t
            :components ((:file "interface")
                         (:file "chord")
                         (:file "utils")))))
