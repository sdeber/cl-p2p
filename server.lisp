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
	    (get-blocksize file) blocksize
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
    (read-sequence buf stream)
    buf))
