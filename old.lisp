(defconstant +buffer-size+ 10)

(defun hton-vector (num dest pos)
  (do ((x 0 (1+ x))
       (y 0 (incf y 8)))
      ((= x 4) dest)
    (setf (aref dist (+ x pos))
	  (ldb (byte 8 y) num))))

(defun create-message (length id &rest payload)
  (macrolet ((hton-vector (num)
		 `(do ((x 0 (1+ x))
		       (y 0 (incf y 8)))
		      ((= x 4) t)
		   (setf (aref message pos)
			 (ldb (byte 8 y) ,num)
			 pos (1+ pos))))
	     (split-to-bytes (num)
	       `(loop repeat 4
		     for y = 0 then (incf y 8)
		     do (setf (aref message pos) (ldb (byte 8 y) ,num)
			 pos (1+ pos)))))
    (if (= length 0)
	(make-array 1 :element-type '(unsigned-byte 8))
	(let ((pos 0)
	      (message (make-array (+ length 4) :element-type '(unsigned-byte 8))))
	  (hton-vector length)
	  (setf (aref message pos) id
		pos (1+ pos))
	  (cond
	    ((or (= id 4) (= id 9)) (hton-vector (car payload)))
	    ((= id 5) (split-to-bytes (car payload)))
	    ((or (= id 6) (= id 8)) (loop for y in payload do (hton-vector y)))
	    ((= id 7) (hton-vector (car payload))
	     (hton-vector (cdar payload))
	     (read-sequence message (nth 2 payload) :start pos)))
	  message))))
	       
(define-condition buffer-error ()
  ((buffer-id :initarg :buffer-id
		       :reader get-id)))

(define-condition buffer-full (buffer-error))
(define-condition buffer-empty (buffer-error))

(defun hton (num)
  (do ((x 0 (1+ x))
       (y 0 (incf y 8))
       (result 0))
      ((= x 4) tmp)
    (setf (ldb (byte 8 (- 24 y)) result) (ldb (byte 8 y) num))))

(defclass netbuffer ()
  ((buffer :accessor buffer
	   :initarg :buffer
	   :initform (make-array +buffer-size+
		      :element-type '(unsigned-byte 8)
		      :initial-element 0))
   (head :accessor head
	 :initform 0)
   (tail :accessor tail
	 :initform 0)
   (id :accessor id
       :initarg :id)))

(defmethod buffer-freespace ((buf netbuffer))
  ;;Bounded buffer technique, minus is expressed as plus the complement
  ;; head + # of free slots mod buffer-size = tail + buffer-size - 1 mod buffer-size 
  (mod (+ (tail buf) (1- +buffer-size+) (- +buffer-size+ (head buf)))
       +buffer-size+))

(defun send-message (socket message)
  (sb-bsd-socket:socket-send socket message))

(defmethod write-to-buffer ((buf netbuffer) src)
  (let ((freespace (buffer-freespace buf)))
    (if (= 0 freespace)
	(error 'buffer-full :buffer-id (id buf))
	(cond
	  ((typep src '(unsigned-byte 8))
	   (setf (aref (buffer buf) (head buf)) src
		 (head buf) (1+ (head buf))))
	  ((typep src '(simple-array (unsigned-byte 8) *))
	   (loop repeat freespace
	      for x = (head buf) then (mod (1+ x) +buffer-size+)
	      and y across src
	      do (setf (aref (buffer buf) x) y)
	      finally (setf (head buf) (mod (1+ x) +buffer-size+))))))))

(defmethod read-from-buffer ((buf netbuffer) &optional length)
  (if (= (head buf) (tail buf))
      (error 'buffer-empty :buffer-id (id buffer)))
  (let ((queue-tail (tail buf)))
    (if (null length) ;;just read one byte.
	(progn (aref (buffer buf) queue-tail)
	     (setf (tail buf) (mod (1+ queue-tail) +buffer-size+)))
	(let* ((num-of-elements (mod (+ (head buf) (- +buffer-size+ (tail buf)))
				     +buffer-size+))
	       (result (make-array num-of-elements :element-type '(unsigned-byte 8)))
	       (actual (if (< num-of-elements length)
			   num-of-elements
			   length)))
	  (loop repeat actual
	     for x = queue-tail then (mod (1+ x) +buffer-size+)
	     and for y = 0 then (1+ y)
	     do (setf (aref result y) (aref (buffer buf) x))
	     finally (vaules result actual))))))

(defclass bounded-queue ()
  ((queue :accessor queue
	  :initarg :queue
	  :initform (make-array 20 :initial-element nil))
   (head :accessor head
	 :initarg :head
	 :initform 0)
   (tail :accessor tail
	 :initarg :tail
	 :initform 0)
   (size :accessor size-of-queue
	 :initarg :size
	 :initform 20)
   (mutex :accessor queue-mutex
	  :initarg :mutex)
   (waitqueue :accessor wait-queue
	      :initarg :waitqueue)))

(defmethod IsEmpty ((q bounded-queue))
  (if (= (head q) (tail q))
      t
      nil))

(defmethod IsFull ((q bounded-queue))
  (if (= (head q)
	 (mod (+ (tail q) (1- (size-of-queue q)))))
      t
      nil))

(defmethod space ((q bounded-queue))
  (mod (+ (tail q) (1- (size-of-queue q)) (- (size-of-queue q) (head q)))
       (size-of-queue q)))

(defmethod notify ((q bounded-queue))
  (sb-thread:condition-broadcast (wait-queue q)))

(defmethod dequeue ((q bounded-queue) block-p)
  (let ((queue-head (head q)) (queue-tail (tail q)))
    (sb-thread:with-mutex ((queue-mutex q) :wait-p t)
      (if (= queue-head queue-tail)
	  (if (null block-p)
	      (progn (sb-thread:release-mutex (queue-mutex q))
		     (error 'queue-empty))
	      (sb-thread:condition-wait (wait-queue q) (queue-mutex q)))
	  (progn (setf (tail q) (mod (1+ queue-tail) (size-of-queue q)))
		 (aref (queue q) queue-tail))))))

(defmethod enqueue ((q bounded-queue) element block-p)
  (sb-thread:with-mutex ((queue-mutex q) :wait-p t)
    (if (IsFull q)
	(if (null block-p)
	    (progn (sb-thread:release-mutex (queue-mutex q))
		   (error 'queue-full))
	    (sb-thread:condition-wait (wait-queue q) (queue-mutex q)))
	(let ((queue-head (head q)))
	  (setf (head q) (mod (1+ queue-head) (size-of-queue q))
		(aref (queue q) queue-head) element)))))


(defclass multi-threaded-stack ()
  ((queue :accessor queue
	  :initarg :queue
	  :initform (make-array 20 :initial-element nil))
   (id :accessor id
       :initarg :id)
   (head :accessor head
	 :initarg :head
	 :initform -1)
   (count :accessor number-of-queue
	  :initarg :count
	  :initform 0
	  :allocation :class)
   (size :accessor size-of-queue
	 :initarg :size)
   (mutex :accessor queue-mutex
	  :initarg :mutex)
   (waitqueue :accessor wait-queue
	      :initarg :waitqueue)))

(defclass tunnel ()
  ((socket :accessor sock
	   :initarg :sock)
   (remote-peer :accessor remote-peer
		:initarg :remote-peer)
   (send-queue :accessor send-queue
	       :initarg :send-queue)
   (read-queue :accessor read-queue
	       :initarg :read-queue)
   (reader-thread :accessor reader
		  :initarg :reader)
   (writer-thread :accessor writer
		  :initarg :writer)
   (status :accessor status
	   :initarg :status
	   :initform :disconnected)
   (lock-on-socket :accessor lock
		   :initarg :lock)
   (waitqueue :accessor wait-queue
	      :initarg :waitqueue)))

(defmacro define-writer-thread ((tunnel element) &body body)
  `(setf (writer ,tunnel)
	 (sb-thread:make-thread
	  (lambda ()
	    (do ()
		(nil)
	      (let ((,element (dequeue (send-queue ,tunnel))))
		(sb-thread:with-mutex ((lock ,tunnel) :wait-p t)
		,@body)))))))

(defmacro define-reader-thread ((tunnel element) &body body)
  ;;"body" has to bind "element" to some value.
  `(setf (reader ,tunnel)
	 (sb-thread:make-thread
	  (lambda ()
	    (do ()
		(nil)
	      (sb-thread:with-mutex ((lock ,tunnel) :wait-p t)
		,@body)
	      (enqueue (read-queue ,tunnel)
		       ,element))))))

(defclass pool ()
  ((cache :accessor cache
	  :initarg :cache
	  :initform (make-hash-table))))

(defmethod add-to-pool ((p pool) key value)
  (setf (gethash key (cache p)) value))

(defmethod remove-from-pool ((p pool) key)
  (remhash key (cache p)))

(defmacro create-tunnel (sock remote-peer send-queue read-queue)
  `(incf (tunnel-count (make-instance 'tunnel
			:sock ,sock
			:remote-peer ,remote-peer
			:send-queue ,send-queue
			:read-queue ,read-queue))))

(defmethod send-message ((dest tunnel))
  (sb-bsd-sockets:socket-send (sock tunnel) (read-from-queue (send-queue tunnel) nil)))

(defmethod establish-connection ((dest tunnel) remote-peer)
  nil)


