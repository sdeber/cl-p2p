(require :ironclad)
(load "/home/ak70/workspace/assignments/apt/p2p/utility.lisp")
(load "/home/ak70/workspace/assignments/apt/p2p/chord-protocol.lisp")

(defun initialize (ip id &optional (port 7000))
  (let* ((node (make-instance 'chord-node)))
    (update-fingertable node (make-instance 'node
					    :id (ironclad:digest-sequence :sha1 id)
					    :ip ip 
			      :port (hton port)) 0)
    (update-fingertable node (get-local node) 1)
    (setf (get-predecessor node) (get-local node))
    (sb-bsd-sockets:socket-bind (get-socket node) ip port)
    node))
  
(defclass chord-node ()
  ((fingertable :accessor get-fingertable
		:initform (make-array 161)
		:documentation "array(0) stores information about this node")
   (status :accessor get-status
	   :initform :offline)
   (predecessor :accessor get-predecessor
		:initarg :predecessor
		:initform nil)
   (requestpool :accessor get-requestpool
		:initform (make-instance 'data-pool))
   (socket :accessor get-socket
	   :initform (make-instance
		      'sb-bsd-sockets:inet-socket
		      :type :datagram :protocol :udp))
   (server-thread :accessor get-server
		  :initarg :server
		  :initform nil)))

(defmethod get-successor ((node chord-node))
  (aref (get-fingertable node) 1))

(defmethod get-id ((node chord-node))
  (get-id (aref (get-fingertable node) 0)))

(defmethod get-ip ((node chord-node))
  (get-ip (aref (get-fingertable node) 0)))

(defmethod get-port ((node chord-node))
  (get-port (aref (get-fingertable node) 0)))

(defmethod get-local ((node chord-node))
  (aref (get-fingertable node) 0))

(defmethod update-fingertable ((node chord-node) (table node) entry)
  (let ((fingertable (get-fingertable node)))
    (setf (aref fingertable entry) table)))

(defmethod send-message ((sender node) (destination node) content type)
  (with-socket (sock :datagram :udp)
    (let ((message (make-instance 'message
				  :type type
				  :content content
				  :sender sender)))
      (sb-bsd-sockets:socket-send sock (message->vector message) nil
				  :address
				  (list (get-ip destination)
					(ntoh (get-port destination)))))))

(defmethod send-and-get-reply ((peer chord-node) content type known-ip &optional (port 7000))
  (with-socket (sock :datagram :udp)
    (sb-bsd-sockets:socket-connect sock known-ip port)
    (let ((message (create-message (get-id peer)
				   (get-ip peer)
				   (ntoh (get-port peer))
				   content
				   type))
	  (pool (get-requestpool peer)))
      (sb-thread:with-mutex ((get-lock pool))
	(setf (gethash content (get-pool pool)) t)
	(sb-bsd-sockets:socket-send sock (message->vector message) nil)
	(loop with result = nil
	   while (null result)
	   do (progn (sb-thread:condition-wait
		      (get-waiting-queue pool)
		      (get-lock pool))
		     (let ((data (gethash content (get-pool pool))))
		       (if (not (eql data t))
			   (setf result data))))
	   finally (progn (remhash content (get-pool pool)) (return result)))))))

(defmethod find-closest-node ((node chord-node) key)
  ;;find the node which is closest to the given key
  ;;Currently, it just returns the successor.
  (declare (ignore key))
  (aref (get-fingertable node) 1))

(defmethod lookup ((peer chord-node) key)
  (let ((message (send-and-get-reply peer key :search
				     (get-ip (get-successor peer))
				     (ntoh (get-port (get-successor peer))))))
    (if (null message)
	;;search failed
	nil
	(get-sender message))))

(defmethod forward-message ((m message) (peer chord-node))
  (let ((dest-ip (get-ip (get-successor peer)))
	(dest-port (ntoh (get-port (get-successor peer))))
	(message (message->vector m)))
    (with-socket (sock :datagram :udp)
      (sb-bsd-sockets:socket-send
       sock message nil
       :address (list dest-ip dest-port)))))

(defmethod server ((peer chord-node))
  (let ((requestpool (get-requestpool peer))
	(myself (get-local peer))
	(my-id (get-id peer))
	(sock (get-socket peer))
	(message (make-array 47 :element-type '(unsigned-byte 8))))
    (labels ((process-search (message)
	       (let* ((sender (get-sender message))
		      (key (get-content message)))
		 (if (or (in-interval (get-id (get-predecessor peer))
				      my-id
				      (get-content message))
			 (equalp (get-id (get-successor peer)) my-id))
		     (send-message myself sender key :answer)
		     (forward-message message peer))))
	     (search-successor (message)
	       (if (or (in-interval (get-id (get-predecessor peer))
				    my-id
				    (get-id (get-sender message)))
		       (equalp (get-id (get-predecessor peer)) my-id)) ;;no predecessor.
		   ;;this node is your successor
		 (send-message myself (get-sender message) (get-content message) :answer)
		 (forward-message message peer)))
	     (process-answer (message)
	       (sb-thread:with-mutex ((get-lock requestpool))
		 (setf (gethash (get-content message) (get-pool requestpool))
		       message)
		 (sb-thread:condition-broadcast (get-waiting-queue requestpool)))))
      (setf (get-server peer)
	    (sb-thread:make-thread
	     (lambda ()
	       (loop 
		  for data = (sb-bsd-sockets:socket-receive sock message nil
			      :element-type '(unsigned-byte 8)) 
		  then (sb-bsd-sockets:socket-receive sock message nil
			:element-type '(unsigned-byte 8))
		  do (let ((m (vector->message message)))
		       (case (get-type m)
			 (:search (process-search m))
			 (:join (search-successor m))
			 (:stablize (send-message (get-predecessor peer) (get-sender m)
						  (get-content m) :answer))
			 (:answer (process-answer m))
			 (:notify (progn (setf (get-predecessor peer) (get-sender m)))))))))))))
		    
(defmethod join ((node chord-node) known-ip &optional (port 7000))
  (let ((message (send-and-get-reply node (get-id node) :join known-ip port)))
    (if (null message)
	;;failed
	nil
	(setf (aref (get-fingertable node) 1) (get-sender message)))))

(defmethod stablize ((node chord-node))
  (let ((message (send-and-get-reply node (get-id node) :stablize
				     (get-ip (get-successor node))
				     (ntoh (get-port (get-successor node))))))
    (if (null message)
	;;stablize failed, rejoin the system.
	nil
	(let* ((sender (get-sender message))
	      (sp (get-id sender))
	      (successor (get-id (get-successor node)))
	      (id (get-id node)))
	  (if (or (equalp successor id) (in-interval id successor sp))
	    ;;update my successor.
	      (update-fingertable node sender 1))
	  (send-message (get-local node) (get-successor node) (get-id node) :notify)))))

(defun start-system (my-id my-ip my-port known-ip known-port)
  (let* ((myself (initialize my-ip my-id my-port))
	 (stablize-timer (sb-ext:make-timer (lambda ()
					      (if (null (stablize myself))
						  (setf (get-status myself) :offline)
						  (setf (get-status myself) :online))))))
    (if (null (join myself known-ip known-port))
	nil
	(sb-ext:schedule-timer stablize-timer 120 :repeat-interval 120))))

(defun create-peers (start-port num)
  (loop for port from start-port to (+ start-port num)
       collect (initialize #(127 0 0 1) (hton port) port)))
(defun form-net (peerlist)
  (let ((well-known-ip (get-ip (car peerlist)))
	(well-known-port (get-port (car peerlist))))
    (loop for peer in peerlist
	 do (server peer))
    (loop for peer in (cdr peerlist)
	 do (join peer well-known-ip (ntoh well-known-port)))))
(defun stablize-all (peerlist)
  (loop repeat 10
       do (progn (sleep 1)
		 (loop for peer in peerlist
		      do (progn (stablize peer))))))
(defun cleanup (peerlist)
  (loop for peer in peerlist
     do (progn (sb-thread:terminate-thread (get-server peer))
	       (sb-bsd-sockets:socket-close (get-socket peer)))))

(defun checkp (peerlist)
  (loop for peer in peerlist
     collect (bytevector->bignum (get-id (get-predecessor peer)))))
(defun checks (peerlist)
  (loop for peer in peerlist
    collect (bytevector->bignum (get-id (get-successor peer)))))
(defun checki (peerlist)
  (loop for peer in peerlist
     collect (bytevector->bignum (get-id peer))))

(defun tranverse (peer peerlist head &optional (count 0))
  (labels ((find-peer (id peerlist)
	     (loop for peer in peerlist
		when (= (bytevector->bignum (get-id peer)) id)
		do (return peer)
		  finally (return nil))))
    (cond ((null peer) nil)
      ((= (bytevector->bignum (get-id (get-successor peer))) head) (values t count))
      (t (tranverse (find-peer (bytevector->bignum (get-id (get-successor peer)))
			       peerlist)
		    peerlist head (1+ count))))))
(defpackage :chord
  (:use :sb-bsd-sockets :sb-thread :ironclad :common-lisp :utility
	:chord-protocol)
  (:export lookup initialze))



