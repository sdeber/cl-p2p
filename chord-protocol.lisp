
(defvar *type-list* (list :search :answer :join :stablize :notify :quit))
(defclass node ()
  ((id :accessor get-id
       :initarg :id
       :documentation
       "identifier of a node, an array of type unsigned byte of 
length 20")
   (ip :accessor get-ip
       :initarg :ip)
   (port :accessor get-port
	 :initarg :port
	 :documentation "ports are represented by byte-arrays in network byte order.")))

(defclass message ()
  ((type :accessor get-type
	 :initarg :type
	 :documentation ":search,:answer,:join,:failure,:predecessor,:quit,:notify")
   (content :accessor get-content
	    :initarg :content
	    :documentation "it is a key")
   (sender :accessor get-sender
	   :initarg :sender
	   :documentation "the sender of the message, it is of type node")))

(defmethod message->vector ((m message))
  "Each quary is of length 47 bytes,it is orgnised as follows: 
type(0,1 or 2) + key + sender's id + sender's ip + sender's port.
For type 0, it is a search of data, so the key is the key of the data,
for type 1, it is a answer to a search, the key is received from the 
search, for type 2, it is a join-request, the sender uses its id as the
key,type 3 means search failed."
  (let ((type-array (make-array 1 :element-type '(unsigned-byte 8)))
	(sender (get-sender m)))
      (setf (aref type-array 0) (position (get-type m) *type-list*))
      (concatenate '(vector (unsigned-byte 8) *) 
		   type-array
		   (get-content m)
		   (get-id sender) (get-ip sender) (get-port sender))))

(defun vector->message (message-vector)
  (let ((m (make-instance 'message))
	(sender (make-instance 'node)))
    (setf (get-type m) (nth (aref message-vector 0) *type-list*)
	  (get-content m) (subseq message-vector 1 21)
	  (get-id sender) (subseq message-vector 21 41)
	  (get-ip sender) (subseq message-vector 41 45)
	  (get-port sender) (subseq message-vector 45 47)
	  (get-sender m) sender)
    m))

(defun create-message (sender-id sender-ip sender-port content type)
  (make-instance 'message :type type
			  :content content
			  :sender (make-instance 'node :id sender-id
						       :ip sender-ip
						       :port (hton sender-port))))
(defpackage :chord-protocol
  (:use :common-lisp)
  (:export "message" "message->vector" "vector->message"
	   "create-message"))
