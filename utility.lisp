(defun ntoh (buffer)
  (let ((length (array-total-size buffer)))
    (loop with result = 0
       for x across buffer
       and y = (* (1- length) 8) then (- y 8)
       do
       (setf (ldb (byte 8 y) result) x)
       finally (return result))))

(defun hton (num &optional (width 2))
  (loop with result = (make-array width :element-type '(unsigned-byte 8))
       for x from 0 below width
       and y = (* (1- width) 8) then (- y 8)
       do (setf (aref result x) (ldb (byte 8 y) num))
       finally (return result)))

(defun bytevector->bignum (v)
  "Little endian"
  (loop with result = 0
     for x across v
     and y = 0 then (+ y 8)
     do (setf  (ldb (byte 8 y) result) x)
     finally (return result)))

(defmacro with-socket ((socketname type protocol) &body body)
  `(let ((,socketname (make-instance
		       'sb-bsd-sockets:inet-socket
		       :type ,type :protocol ,protocol)))
     (unwind-protect (progn ,@body)
       (sb-bsd-sockets:socket-close ,socketname))))

(defmacro with-client-socket ((socketname host port protocol &key element-type timeout deadline nodelay local-host local-port) &body body)
  `(let ((,socketname (usocket:socket-connect ,host ,port :protocol ,protocol :element-type ,element-type :timeout ,timeout :deadline ,deadline :nodelay ,nodelay :local-host ,local-host :local-port ,local-port)))
     (unwind-protect (progn ,@body)
       (usocket:socket-close ,socketname))))

(defmacro with-socket-stream ((streamname type protocol ip port) &body body)
  `(let ((sock (make-instance 'sb-bsd-sockets:inet-socket
			      :type ,type :protocol ,protocol)))
     (sb-bsd-sockets:socket-connect sock ,ip ,port)
     (let ((,streamname (sb-bsd-sockets:socket-make-stream
			 sock
			 :input t
			 :output t
			 :buffering :full
			 :element-type '(unsigned-byte 8))))
       (unwind-protect (progn ,@body)
	 (close stream)))))

(defun calculate-distance (key id)
  (mod (+ (bytevector->bignum id)
	  (1+ (loop with result = 0
		 for x across key
		 and y = 0 then (+ y 8)
		 do (setf (ldb (byte 8 y) result) (lognot x))
		 finally (return result))))
       (expt 2 160)))

(defun in-interval (lowerbound upperbound key)
  "This function is used to test wether a key should be stored in the node
whose id is upperbound, or a node with id KEY should be the direct predecessor
of the node with id UPPERBOUND. NOTE:distance is calculated clockwise."
  (let ((distance-to-lowerbound (calculate-distance key lowerbound))
	(distance-to-upperbound (calculate-distance key upperbound)))
    (if (< distance-to-upperbound distance-to-lowerbound)
	t
	nil)))

(defclass data-pool ()
  ((pool :accessor get-pool
	 :initform (make-hash-table :test #'equalp))
   (lock :accessor get-lock
	 :initform (sb-thread:make-mutex))
   (waitingqueue :accessor get-waiting-queue
		 :initform (sb-thread:make-waitqueue))))

(defmethod get-data ((pool data-pool) key)
  (sb-thread:with-mutex ((get-lock pool))
    (gethash key (get-pool pool))))

(defmethod write-data ((pool data-pool) key data)
  (sb-thread:with-mutex ((get-lock pool))
    (setf (gethash key (get-pool pool)) data)))

(defmethod remove-data ((pool data-pool) key)
  (sb-thread:with-mutex ((get-lock pool))
    (remhash key (get-pool pool))))

(let ((init 0))
  (defun generate-requestID (upperbound)
    (mod (incf init) upperbound)))

(defclass message ()
  ((total-size :accessor get-total-size
	       :documentation "Store the total size of a message in bytes")))
   

(defclass chordMessage (message)
  ((messageID :accessor get-messageID)
   (senderID :accessor get-senderID)
   (senderIP :accessor get-senderIP)
   (senderPort :accessor get-senderPort)
   (targetID :accessor get-targetID)))

(defun integer->bytevector (i size &optional bytevector)
  (if (null bytevector)
      (setf bytevector (make-array size :element-type '(unsigned-byte 8))))
  (loop for x = 0 below size
     and y = 0 then (+ y 8)
     do (setf (aref bytevector x) (ldb (byte 8 y) i)))
  bytevector)

(defconst int-begin (char-int #\i))
(defconst end-suffix (char-int #\e))
(defconst list-begin (char-int #\l))
(defconst dict-begin (char-int #\d))	
(defun bencodeInteger (num)
  "Convert an integer of length less than or equal to 4 bytes into a bencoded integer"
  (concatanate 'string "i" (write-to-string num) "e"))

(defun bencodeString (s)
  "Bencode a string"
  (let ((length (array-total-size s)))
    (concatenate 'string (write-to-string length) ":" s)))

(defun bencodeList (lst)
  (loop
     with result = "l:"
     for e in lst
     do (setf result (concatenate 'string result e))
     finally (concatenate 'string result "e")))

(defmethod bencode ((m chordMessage) &optional result)
  (let ((byteMessageID (integer->btevector (get-messageID m)))
	(byteSenderID (integer->bytevector (get-senderID m)))
	(byteSenderIP (integer->bytevector (get-senderIP m)))
(defmethod bencoded->message ((targetMessage chordMessage) buf)
  )

(defclass connection ()
  ((socket :acessor get-socket
	   :initarg :socket
	   :initform nil)
   (type :accessor get-type
	 :initarg :type
	 :documentation "Specify the type of the socket, e.g. TCP, UDP, UNIX, etc")))

(defclass sbcl_connection (connection))

(defmethod initialize-instance :after ((connection sbcl_connection) &key)
  (if (and (null (get-socket connection))
	   (eq (get-type connection) :tcp))
      (setf (get-type connection) (make-instance 'sb-bsd-sockets:inet-socket
						 :type :stream :protocol :tcp))
      (setf (get-type connection) (make-instance 'sb-bsd-sockets:inet-socket
						 :type :datagram :protocol :udp))))

(defgeneric open-connection (connection &optional ip port)
   (:documentation "Open the given connection"))

(defgeneric close-connection (connection)
  (:documentation "Close the given connection"))

(defgeneric send-message (message connection)
  (:documentation "Send a message though the given connection"))

(defclass connection_pool ()
  ((lock :accessor get-lock
	 :documentation "a mutex to make the pool is thread safe")
   (limit :accessor get-limit
	  :initform 4
	  :documentation "The max number of connection")
   (connection_list :accessor get-connections
		    :documentation "The data structure that holds the connections")))

(defgeneric get-connection (pool)
  (:documentation "Get a connection from the pool"))

(defgeneric return-connection (usd_connection pool)
  (:documentation "Return a connection to the pool"))
			       


(defpackage :utility
  (:use :common-lisp)
  (:export "ntoh" "hton" "bytevector->bignum"
	   "with-socket" "calculate-distance" "in-interval"))
