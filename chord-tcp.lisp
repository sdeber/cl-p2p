
(defclass node ()
  ((id :accessor get-id
       :initarg :id
       :documentation
       "identifier of a node, an array of type unsigned byte of 
length 20")
   (ip :accessor get-ip
       :initarg :ip)
   (port :accessor get-port
	 :initarg :port)))

(defclass quary-pool ()
  ((pool :accessor get-pool
	 :initform (make-hash-table :test #'equal))
   (lock :accessor get-lock
	  :initform (sb-thread:make-mutex))
   (waitqueue :accessor get-waitqueue
	      :initform (sb-thread:make-waitqueue)))
  (:documentation "When a thread fires a search, it adds the key to the pool,
if the search is answered, the server will try to find the key in the pool, if
no such a key is found, the connection is dropped."))

(defclass chord-node ()
  ((fingertable :accessor get-fingertable
		:initform (make-array 161)
		:documentation "array(0) stores information about this node")
   (predecessor :accessor get-predecessor
		:initarg :predecessor
		:initform nil)
   (quarypool :accessor get-quarypool
	      :initform (make-instance 'quary-pool))
   (datapool :accessor get-datapool
	     :initform (make-hash-table :test #'equal))
   (socket :accessor get-socket
	   :initform (make-instance
		      'sb-bsd-sockets:inet-socket
		      :type :stream :protocol :tcp))))

(defmethod get-id ((node chord-node))
  (get-id (aref (get-fingertable node) 0)))

(defmethod get-port ((node chord-node))
  (get-port (aref =get-fingertable node) 0))

(defmethod get-local ((node chord-node))
  (aref (get-fingertable node) 0))

(defmethod update-fingertable ((node chord-node) (table node) entry)
  (let ((fingertable (get-fingertable node)))
    (setf (aref fingertable entry) table)))

(defclass message ()
  ((type :accessor get-type
	 :initarg :type
	 :documentation ":search,:answer,:join")
   (content :accessor get-content
	    :initarg :content
	    :documentation "it is a key")
   (sender :accessor get-sender
	   :initarg :sender)))

(defmethod message->vector ((m message))
  "Each quary is of length 47 bytes,it is orgnised as follows: 
type(0,1 or 2) + key + sender's id + sender's ip + sender's port.
For type 0, it is a search of data, so the key is the key of the data,
for type 1, it is a answer to a search, the key is received from the 
search, for type 2, it is a join-request, the sender uses its id as the
key."
  (let ((type-array (make-array 1 :element-type '(unsigned-byte 8)))
	(sender (get-sender m)))
    (case (get-type m)
      (:search (setf (aref type-array 0) 0))
      (:answer (setf (aref type-array 0) 1))
      (:join (setf (aref type-array 0) 2)))
    (concatenate '(vector (unsigned-byte 8) *) 
		 type-array
		 (get-content m)
		 (get-id sender) (get-ip sender) (get-port sender))))

(defmethod vector->message ((m message) message-vector)
  (let ((type-list (list :search :answer :join))
	(sender (get-sender m)))
    (setf (get-type m) (nth (aref message-vector 0) type-list)
	  (get-content m) (subseq message-vector 1 21)
	  (get-id sender) (subseq message-vector 21 41)
	  (get-ip sender) (subseq message-vector 41 45)
	  (get-port sender) (subseq message-vector 45))
    m))

(defmethod create-message ((sender node) key &key type)
  (make-instance 'message :type tyoe :content key :sender sender))

(defmethod send-message ((node chord-node) key ip port &key type)
  (let ((sock (make-instance
	       'sb-bsd-sockets:inet-socket
	       :type :stream :protocol :tcp))
	(message (message->vector (create-message (get-local node) key type)))
	(p (get-quarypool node)))
    (sb-bsd-sockets:socket-connect sock ip port)
    (if (or (eql type :search) (eql type :join))
	(let ((result nil) (lock (get-lock p)))
	  (sb-thread:with-mutex (lock)
	    (setf (gethash key (get-quarypool p)) :waiting)
	    ;;if the network is slow, it is not efficient.
	    (sb-bsd-sockets:socket-send sock message nil)
	    (sb-thread:condition-wait (get-waitqueue p) lock)
	    (setf result (gethash key (get-quarypool p)))
	    (remhash key (get-quarypool p)))
	  result)
	;;for answering quaries, we just reply to the sender, no need to wait.
	;;to denote a search failure, use illegl ip address.
	(progn (sb-bsd-sockets:socket-send sock message nil) nil))))

(defmethod server ((node chord-node))
  (labels ((process-answer (vmessage)
	     (let* ((message (vector->message vmessage))
		    (quarypool (get-quarypool node))
		    (key (get-content message)))
	       ;;get mutex first,then process.
	       ))
	   (process-search (vmessage)
	     (let* ((message (vector->message vmessage))
		    (sender (get-sender message))
		    (datapool (get-datapool node))
		    (key (get-content message))
		    (result (gethash key datapool)))
	       (if (null result)
		   (forward message)
		   (send-message node key (get-ip sender) (ntoh (get-port sender)) :answer))))
  (let ((pool (get-quarypool node))
	(sock (get-socket node))
	(message (make-array 47 :element-type '(unsigned-byte 8))))
    (sb-thread:make-thread (lambda ()
			     (loop 
				for socket = (sb-bsd-sockets:socket-accept sock)
				then (sb-bsd-sockets:socket-accept sock)
				do (progn (sb-bsd-sockets:socket-receive socket message nil
					   :element-type '(unsigned-byte 8))
					  (case (aref message 0)
					    (0 (process-search message))
					    (1 (process-answer message))
					    (2 (search-sucessor message)))
					  (sb-bsd-sockets:socket-close socket)))))))

(defmethod join ((node chord-node) known-ip &optional (port 7000))
  (let ((successor (send-message node (get-id node) known-ip port :join)))
    (if (eql successor :failed)
	nil
	)))

(defun initialize (ip &optional (port 7000))
  (let* ((node (make-instance 'node))
	 (sock (get-socket node)))
    (update-fingertable node (make-instance '
			       node
			       :id (ironclad:digest-sequence ip)
			       :ip ip
			       :port (hton port)) 0)
    (sb-bsd-sockets:socket-bind sock node ip port)
    (sb-bsd-sockets:socket-listen sock 20)
    node))