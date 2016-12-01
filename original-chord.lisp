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
    (sb-bsd-sockets:socket-bind (get-datasock node) ip port)
    (sb-bsd-sockets:socket-listen (get-datasock node) 20)
    node))

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
  
(defclass chord-node ()
  ((fingertable :accessor get-fingertable
		:initform (make-array 161)
		:documentation "array(0) stores information about this node")
   (predecessor :accessor get-predecessor
		:initarg :predecessor
		:initform nil)
   (datapool :accessor get-datapool
	     :initform (make-instance 'data-pool))
   (requestpool :accessor get-requestpool
		:initform (make-instance 'data-pool))
   (socket :accessor get-socket
	   :initform (make-instance
		      'sb-bsd-sockets:inet-socket
		      :type :datagram :protocol :udp))
   (datasock :accessor get-datasock
	     :initform (make-instance
			'sb-bsd-sockets:inet-socket
			:type :stream :protocol :tcp))
   (data-thread :accessor get-datathread
		:initarg :datathread
		:initform nil)
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

(defmethod publish ((peer chord-node) key content)
  (let ((message (send-and-get-reply peer key :publish (get-ip (get-successor peer))
				     (ntoh (get-port (get-successor peer))))))
    (if (null message)
	nil
	(let* ((sender (get-sender message))
	       (ip (get-ip sender))
	       (port (ntoh (get-port sender))))
	  (with-socket (sock :stream :tcp)
	    (sb-bsd-sockets:socket-connect sock ip port)
	    (let ((stream (sb-bsd-sockets:socket-make-stream
			   sock :input t :output t
			   :element-type '(unsigned-byte 8)))
		  (length (hton (+ 21 (array-total-size content)) 4)))
	      (write-sequence length stream)
	      (write-byte 1 stream)
	      (write-sequence key stream)
	      (write-sequence content stream)
	      (write-sequence (make-array 4 :element-type '(unsigned-byte 8)) stream)
	      (force-output)
	      (close stream)
	      t))))))

(defmethod find-closest-node ((node chord-node) key)
  ;;find the node which is closest to the given key
  ;;Currently, it just returns the successor.
  (declare (ignore key))
  (aref (get-fingertable node) 1))

(defmethod lookup ((peer chord-node) key)
  (let ((message (send-and-get-reply peer key :search
				     (get-ip (get-successor *myself*))
				     (ntoh (get-port (get-successor *myself*))))))
    (if (or (null message) (eql (get-type message) :failure))
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
  (let ((datapool (get-datapool peer))
	(requestpool (get-requestpool peer))
	(myself (get-local peer))
	(my-id (get-id peer))
	(sock (get-socket peer))
	(message (make-array 47 :element-type '(unsigned-byte 8)))
	(datasock (get-datasock peer)))
    (labels ((process-search (message)
	       (let* ((sender (get-sender message))
		      (key (get-content message))
		      (distance (calculate-distance key my-id)))
		 (if (<= distance (calculate-distance key (get-id sender)))
		   (let ((result (get-data datapool key)))
		     (if (null result)
			 (send-message myself sender key :failure)
			 (send-message myself sender key :answer)))
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
	       (sb-thread:condition-broadcast (get-waiting-queue requestpool))))
	   (process-failure (message)
	     (sb-thread:with-mutex ((get-lock requestpool))
	       (setf (gethash (get-content message) (get-pool requestpool)) nil)
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
			 (:publish (if (in-interval (get-id (get-predecessor peer))
						    my-id
						    (get-content m))
				       (send-message myself (get-sender m) (get-content m) :answer)
				       (forward-message m peer)))
			 (:answer (process-answer m))
			 (:failure (process-failure m))
			 (:notify (progn (setf (get-predecessor peer) (get-sender m))
					 ;(balance-key peer (get-sender m))
					 )))))))))
      (setf (get-datathread peer)
	    (sb-thread:make-thread
	     (lambda ()
	       (loop for newsock = (sb-bsd-sockets:socket-accept datasock)
		  then (sb-bsd-sockets:socket-accept datasock)
		  do (sb-thread:make-thread
		      (lambda ()
			(process-datatransfer newsock datapool)))))))))
		    
(defun process-download-request (key datapool stream)
  (let* ((data (get-data key datapool))
	 (data-length (hton (1+ (array-total-size data)))))
    ;;error handler should be added here.
   (write-sequence data-length stream)
   (write-byte 1 stream)
   (write-sequence data stream)
   (force-output)))

(defun process-upload-request (key data datapool)
  (write-data datapool key data))

(defun process-datatransfer (sock datapool)
  (let ((stream (sb-bsd-sockets:socket-make-stream
		 sock :input t
		      :output t
		      :element-type '(unsigned-byte 8)
		      :buffering :none))
	(length-n (make-array 4 :element-type '(unsigned-byte 8))))
    (loop
       while t
       do (let ((length (ntoh (progn (read-sequence length-n stream)
				     length-n))))
	    (if (= length 0)
		;;transfer ended.
		(progn (close stream) (return))
		(let ((buf (make-array length :element-type '(unsigned-byte 8))))
		  (read-sequence buf stream)
		  (cond
		    ((= (aref buf 0) 0)
		     ;;download request
		     (process-download-request (subseq buf 1) datapool stream))
		    ((= (aref buf 0) 1)
		     ;;upload request
		     (process-upload-request (subseq buf 1 21) (subseq buf 21) datapool))
		    (t (close stream) (return)))))))))

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

(defmethod balance-key ((node chord-node) (destination node))
  (with-socket (sock :stream :tcp)
    (sb-bsd-sockets:socket-connect sock (get-ip destination) (get-port destination))
    (let ((pool (get-pool (get-data pool node)))
	  (id (get-id node))
	  (target-id (get-id destination))
	  (result nil)
	  (stream (sb-bsd-sockets:socket-make-stream sock
						     :input t
						     :output t
						     :element-type '(unsigned-byte 8)
						     :buffering :none)))
      (sb-thread:with-mutex ((get-lock (get-datapool node)))
	(loop 
	   for datakey being the hash-keys in pool
	   when (< (calculate-distance datakey target-id)
		   (calculate-distance datakey id))
	   do (progn (push (list key (gethash key pool)) result)
		     (remhash key pool))))
      (loop for (key value) in result
	   until
	 do (progn (write-sequence (hton (+ 21 (array-total-size value))) stream)
		   (write-byte 1 stream)
		   (write-sequence key stream)
		   (write-sequence value stream)
		   (force-output stream)))
      (close stream))))

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
	       (sb-bsd-sockets:socket-close (get-socket peer))
	       (sb-thread:terminate-thread (get-datathread peer))
	       (sb-bsd-sockets:socket-close (get-datasock peer)))))

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

