
(defclass file-record ()
  ((filename :accessor get-filename
	     :initarg :filename)
   (filesize :accessor get-filesize
	     :initarg :filesize)
   (filestream :accessor get-filestream
	       :initarg :stream)
   (blocksize :accessor get-blocksize
	      :initarg :blocksize
	      :documentation
	      "the file is splited into a number of fixed length blocks")
   (num-of-blocks :accessor get-num
		  :initarg :num)
   (filehash :accessor get-filehash
	     :initarg :filehash
	     :documentation "Identify the file")
   (checksums :accessor get-checksum-array
	      :initarg :checksums
	      :documentation
	      "Each block has a checksum, this slot is an array of all checksums"))
  (:documentation "This is a class representing a file"))

(defmethod load-file ((file file-record) filename &optional (blocksize 512))
  (labels ((fill-checksums (array-size stream)
	     (let ((checksum-array
		    (make-array array-size))
		   (tmp (make-array (* blocksize 1024)
			 :element-type '(unsigned-byte 8))))
	       (loop
		  for x from 0 below array-size
		  and actual-size = (read-sequence tmp stream)
		  then (read-sequence tmp stream)
		  do (setf (aref checksum-array x)
			    (ironclad:digest-sequence :sha1
						      (subseq tmp 0 actual-size))))
	       checksum-array)))
    (let ((stream (open filename :element-type '(unsigned-byte 8)))
	  (hash (ironclad:digest-file :sha1 filename)))
      (setf (get-filename file) filename
	    (get-filesize file) (file-length stream)
	    (get-filestream file) stream
	    (get-blocksize file) (* blocksize 1024)
	    (get-num file) (ceiling (/ (get-filesize file) (* blocksize 1024)))
	    (get-filehash file) hash
	    (get-checksum-array file) (fill-checksums (get-num file) stream))))
  file)

(defmacro piece-position (blocknum blocksize offset)
  `(+ (* ,blocknum ,blocksize) ,offset))

(defmethod get-piece ((file file-record) blocknum offset length)
  (let ((buf (make-array length :element-type '(unsigned-byte 8)))
	(start (piece-position blocknum (get-blocksize file) offset))
	(stream (get-filestream file)))
    (file-position stream start)
    (let ((actual-length (read-sequence buf stream)))
      (if (< actual-length length)
	  (subseq buf 0 actual-length)
	  buf))))

(defclass peer ()
  ((datapool :accessor get-datapool
	     :initform (make-instance 'data-pool))
   (server-sock :accessor get-serversock
		:initform (make-instance 'sb-bsd-sockets:inet-socket
					 :type :stream :protocol :tcp))
   (server-thread :accessor get-server)
   (chordnode :accessor get-node
	      :initarg :node)))

(defun create-peer (ip port)
  (let* ((chord (initialize ip (hton port) port))
	 (p (make-instance 'peer :node chord)))
    (sb-bsd-sockets:socket-bind (get-serversock p) ip port)
    (sb-bsd-sockets:socket-listen (get-serversock p) 20)
    (start-server p)
    p))

(defclass semaphore ()
  ((lock :accessor get-lock
	 :initform (sb-thread:make-mutex))
   (waiting-queue :accessor get-waiting-queue
		  :initform (sb-thread:make-waitqueue))
   (count :accessor get-count
	  :initform 0
	  :initarg :count)
   (count-limit :accessor get-limit
		:initform 10
		:initarg :limit)))

(defmethod wait ((s semaphore))
  (sb-thread:with-mutex ((get-lock s))
    (if (>= (get-count s) (get-limit s))
	(progn
	  (sb-thread:condition-wait (get-waiting-queue s) (get-lock s))
	  (incf (get-count s)))
	(incf (get-count s)))))

(defmethod notify ((s semaphore))
  (sb-thread:with-mutex ((get-lock s))
    (decf (get-count s))
    (sb-thread:condition-notify (get-waiting-queue s))))

(defmethod remove-data ((p peer) key)
  (let ((datapool (get-datapool p)))
    (remove-data datapool key)))

(defmethod start-server ((p peer))
  (let ((datasock (get-serversock p))
	(datapool (get-datapool p)))
    (setf (get-server p)
	  (sb-thread:make-thread
	   (lambda ()
	     (loop for newsock = (sb-bsd-sockets:socket-accept datasock)
		then (sb-bsd-sockets:socket-accept datasock)
		do (sb-thread:make-thread
		    (lambda ()
		      (process-datatransfer newsock datapool
					    p)))))))))

(defun process-download-request (key datapool stream)
  (let* ((data (get-data datapool key))
	 (data-length (hton (array-total-size data) 4)))
   (write-sequence data-length stream)
   (write-sequence data stream)
   (force-output stream)))

(defun process-upload-request (key data datapool)
  (write-data datapool key data))

(defun process-datatransfer (sock datapool peer)
  (let ((stream (sb-bsd-sockets:socket-make-stream
		 sock :input t
		      :output t
		      :element-type '(unsigned-byte 8)
		      :buffering :full))
	(length-n (make-array 4 :element-type '(unsigned-byte 8))))
    (handler-case
	(let ((length (ntoh (progn (read-sequence length-n stream)
				   length-n))))
	  (if (= length 0)
	      ;;transfer ended.
	      (close stream)
	      (let ((buf (make-array length :element-type '(unsigned-byte 8))))
		(read-sequence buf stream)
		(cond
		  ((= (aref buf 0) 0)
		   ;;download request
		   (process-download-request (subseq buf 1) datapool stream))
		  ((= (aref buf 0) 1)
		   ;;upload request
		   (process-upload-request (subseq buf 1 21) (subseq buf 21) datapool))
		  ((= (aref buf 0) 2)
		   ;;peer request
		   (balance-key peer (subseq buf 1) datapool))
		  ;;error
		  (t (close stream)))
		(close stream))))
      (nil () (close stream)))))

(defun send (stream type data)
  (let ((length (hton (1+ (array-total-size data)) 4)))
    (write-sequence length stream)
    (write-byte type stream)
    (write-sequence data stream)
    (force-output stream)))

(defmethod balance-key ((p peer) predecessor datapool)
  (if (equalp predecessor
	      (get-id (get-predecessor (get-node p))))
      (let ((pool (get-pool datapool))
	    (result nil)
	    (myid (get-id (get-node p))))
	(sb-thread:with-mutex ((get-lock datapool))
	  (loop 
	     for key being the hash-key in pool
	     when (< (calculate-distance key predecessor)
		     (calculate-distance key myid))
	     do (push (cons key (gethash key pool)) result)))
	(loop for (key.value) in result
	   do (with-socket-stream (stream :stream :tcp (get-ip (get-predecessor
								(get-node p)))
					  (ntoh (get-port (get-predecessor (get-node p)))))
		(send stream 1
		      (concatenate '(vector (unsigned-byte 8) *)
				   key value))
		(remove-data datapool key))))))

(defmethod download-file ((p peer) filename key-list)
  (let ((failure-list nil)
	(filestream (open filename :direction
				   :io
				   :element-type '(unsigned-byte 8))))
    (loop
       for key across key-list
       and piecenum = 0 then (1+ piecenum)
       do  (let ((piece (download-piece p key)))
	       (if (null piece)
		   (push piecenum failure-list)
		   (progn
		     (write-sequence piece filestream)
		     (force-output filestream))))
       finally (return failure-list))))

(defmethod download-piece ((p peer) key)
  (let* ((chord (get-node p))
	 (location (lookup chord key))
	 (length-v (make-array 4 :element-type '(unsigned-byte 8))))
    (if (null location)
	nil
	(with-socket-stream (stream :stream :tcp (get-ip location)
				    (ntoh (get-port location)))
	  (send stream 0 key)
	  (let* ((length (ntoh (progn
				 (read-sequence length-v stream)
				 length-v)))
		 (buf (make-array length :element-type '(unsigned-byte 8))))
	    (read-sequence buf stream)
	    buf)))))

(defmethod upload-piece ((p peer) piece data)
  (let*  ((chord (get-node p))
	  (location (lookup chord piece)))
      (if (null location)
	  nil
	  (with-socket-stream (stream :stream :tcp (get-ip location)
				      (ntoh (get-port location)))
	    (send stream 1 (concatenate '(vector (unsigned-byte 8) *)
					piece data))))))

(defmethod publish-file ((p peer) (file file-record))
  (let ((blocksize (get-blocksize file))
	(num-of-block (get-num file))
	(piece-list (get-checksum-array file)))
    (loop for piece across piece-list
	 and blocknum from 0 to num-of-block
	 do (upload-piece p piece (get-piece file blocknum 0 blocksize)))))




(defun create-net (peerlist)
  (let ((known-ip (get-ip (get-node (car peerlist))))
	(known-port (get-port (get-node (car peerlist)))))
    (loop for peer in (cdr peerlist)
	 do (join (get-node peer)
		  known-ip
		  (ntoh known-port)))))

(defun stablize-net (peerlist)
  (loop repeat 10
       do (progn (sleep 1)
		 (loop for peer in peerlist
		      do (progn (stablize (get-node peer))
				)))))

(defun create-list-peers (start-port num)
  (loop for port from start-port to (+ start-port num)
     collect (create-peer #(127 0 0 1) port)))
