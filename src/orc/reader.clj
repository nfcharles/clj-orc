(ns orc.reader
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
	    [clojure.core.async :as async]
	    [orc.macro :refer [with-async-record-reader]])
  (import [org.apache.orc OrcFile]
          [org.apache.hadoop.fs Path]
          [org.apache.hadoop.conf Configuration]
          [com.amazonaws AmazonClientException AmazonServiceException]
	  [com.amazonaws.auth DefaultAWSCredentialsProviderChain]
          [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
          [com.amazonaws.services.s3.model PutObjectRequest
                                           AbortMultipartUploadRequest
					   CompleteMultipartUploadRequest
					   InitiateMultipartUploadRequest
					   InitiateMultipartUploadResult
					   PartETag
					   UploadPartRequest
					   UploadPartResult])
  (:gen-class))


(defn credentials []
  (DefaultAWSCredentialsProviderChain.))

(defn client ^AmazonS3Client [^DefaultAWSCredentialsProviderChain creds]
  (AmazonS3Client. creds))

(defn initiate-multipart-upload-request ^InitiateMultipartUploadRequest [bkt key]
  (InitiateMultipartUploadRequest. bkt key))

(defn initiate-multipart-upload-result ^InitiateMultipartUploadResult [^AmazonS3Client clnt ini-request]
  (.initiateMultipartUpload clnt ini-request))

(defn upload-part-request [bkt key uid i ^java.io.File file]
  (println (format "PART-I=%d BYTES=%d" i (.length file)))
  (-> (UploadPartRequest. )
      (.withBucketName bkt)
      (.withKey key)
      (.withUploadId uid)
      (.withPartNumber i)
      (.withFileOffset 0)
      (.withFile file)
      (.withPartSize (.length file))))

(defn upload ^UploadPartResult [^AmazonS3Client clnt bkt key uid part-n part-file]
  (let [req (upload-part-request bkt key uid part-n part-file)]
    (println (format "Uploading fragment %d" part-n))
    (time
      (.uploadPart clnt req))))

(defn complete-multipart-upload-request [bkt key uid etags]
  ;(doseq [[i etag] (map vector (range (count etags)) etags)]
  ;  (println (format "ETAG-%d=%s" i etag)))
  (CompleteMultipartUploadRequest. bkt key uid etags))

(defn abort-multipart-upload-request [bkt key uid]
  (AbortMultipartUploadRequest. bkt key uid))

(defn upload-id [^AmazonS3Client clnt bkt key]
  (->> (initiate-multipart-upload-request bkt key)
       (initiate-multipart-upload-result clnt)
       (.getUploadId)))

(defn write-part ^java.io.File [dest-path  data]
  (with-open [wtr (io/writer dest-path)]
    (try
      (.write wtr data)
      (catch Exception e
        (println e))))
  (io/file dest-path))

(defn start-multipart-writer [pipe thread-count ^AmazonS3Client clnt dest-path-prefix bkt key]
  (let [out-ch (async/chan)
        uid (upload-id clnt bkt key)
        etags (atom (sorted-map))
        active-threads (atom thread-count)]
    (dotimes [thrd-n thread-count]
      (async/thread
        (println (format "Starting multipart writer thread-%d..." thrd-n))
        (try
          (loop []
            (if-let [payload (async/<!! pipe)]
              (let [[part-n fragment] payload
                    part-file (write-part (format "%s-part-%d" dest-path-prefix part-n) fragment)
                    res (upload clnt bkt key uid part-n part-file)]
                (.delete part-file)
                (swap! etags assoc part-n (.getPartETag res))
                (recur))
              (do
                (swap! active-threads dec)
		(when (= @active-threads 0)
                  (async/>!! out-ch :close)
                  (async/close! out-ch)
                  (.completeMultipartUpload clnt (complete-multipart-upload-request bkt key uid (map #(% 1) (seq @etags))))))))
          (catch java.lang.Exception e
            (println e)
            (println "Aborting multipart upload")
            (async/close! pipe) ; shutdown commuincation from producer
            (.abortMultipartUpload clnt (abort-multipart-upload-request bkt key uid))))))
    out-ch))

(defn write [dest-path payload thrd-n]
  (if (> (payload 0) 0)
    (with-open [wtr (io/writer dest-path)]
      (println (format "Writing %s" dest-path))
      (try
        (.write wtr (payload 1))
        (println (format "Done writing %s" dest-path))
	(catch Exception e
	  (println e)))
      (println (format "count.writer.thread_%s=%s" thrd-n (payload 0))))
    (println (format "Not writing %s; no records" dest-path))))

(defn start-writers [in-ch prefix n]
  (let [out-ch (async/chan)
        active-threads (atom n)
        filename #(format "%s-part-%d-%d" %1 %2 %3)] ; <pfx>-part-<thrd-no>-<grp-n>.json
    (dotimes [thrd-n n]
      (async/thread
        (println (format "Staring writer thread-%d..." thrd-n))
        (loop [grp-n 0]
          (if-let [payload (async/<!! in-ch)]
            (do
              (write (filename prefix thrd-n grp-n) payload thrd-n)
	      (recur (inc grp-n)))
	    (do
              (swap! active-threads dec)
              (when (= @active-threads 0)
                (async/>!! out-ch :close)
                (async/close! out-ch)))))))
    out-ch))


(def buffer-size 100)

(defn raw->trimmed [coll]
  (let [s (json/write-str coll)]
    (subs s 1 (dec (count s)))))

(def config-mapping
  (hash-map
    "fs.file.impl" "org.apache.hadoop.fs.LocalFileSystem"))
(defn orc-config [& ops]
  (let [conf (Configuration.)]
    (println "CONFIGURATION")
    (doseq [[k v] config-mapping]
      (println (format "%s=%s" k v))
      (.set conf k v))
    conf))

(defn get-col [^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat n]
  (nth (.cols bat) n))

(defn value [deser bat col-n row-n]
  (let [col (get-col bat col-n)]
    (deser col row-n)))

(defn row->map [col-config bat row-n]
  (loop [col-n 0
  	 col-conf col-config
         rcrd (transient {})]
    (if-let [conf (first col-conf)]
      (let [val (value (conf :fn) bat col-n row-n)]
        (recur (inc col-n) (rest col-conf) (assoc! rcrd col-n val)))
      (persistent! rcrd))))

(defn rows->map-list [col-config ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch bat]
  (let [n-rows (.count bat)]
    (loop [row 0
           rcrds []]
      (if (< row n-rows)
        (let [rec (row->map col-config bat row)]
          (recur (inc row) (conj rcrds rec)))
        rcrds))))

(defn bsize [^java.lang.String s]
  (count (.getBytes s)))

(defn hdr-info [col-headers bat]
  (let [hdr  (col-headers bat)
        ser  (json/write-str (col-headers bat))
	size (bsize ser)]
    [size ser]))

(defn prepare-part
  ([part-n xs suffix]
    ;(println (format "SUFFIX=%s PART=%d" suffix part-n))
    (if (> (count xs) 0)
      (let [data (clojure.string/join "," xs)]
        (if (= part-n 1)
	  (format "%s%s" data suffix)
	  ;; Need to splice data blocks (at X byte boundary) by separator char
          (format ",%s%s" data suffix)))
      suffix))
  ([part-n xs]
    (prepare-part part-n xs "")))

(defn reader ^org.apache.orc.Reader [path conf]
  (OrcFile/createReader path (OrcFile/readerOptions conf)))

(defn schema ^org.apache.orc.TypeDescription [^org.apache.orc.Reader rdr]
  (.getSchema rdr))

(defn batch ^org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch [^org.apache.orc.TypeDescription sch]
  (.createRowBatch sch))

(defn start-worker [pipe conf ^java.lang.String src-path col-headers col-handlers byte-limit]
  (let [rdr (reader (Path. src-path) conf)
        bat (batch (schema rdr))]
    (with-async-record-reader [rr (.rows rdr)]
      (println "Starting worker thread...")

      ;; Process first batch separately. Need first batch reference for special
      ;; header processing.  JSON field names are swapped for ordinal numbers
      ;; for memory optimization and hence, we need a special header record
      (try
        (loop [byte-total 0
               bat-n 1
	       part-n 1
               acc []]
          (if (.nextBatch rr bat)
	    (let [ser (raw->trimmed (rows->map-list (col-handlers bat) bat))
	          ^long size (bsize ser)
	          cur-size (+ size byte-total)]
	      (println (format "BATCH=%d SIZE=%d" bat-n size))
	      (if (= bat-n 1)
	        ;; Handle first batch which requires prepended header info
	        (let [[^long sz sr] (hdr-info col-headers bat)]
	          (if (< (+ sz cur-size) byte-limit)
		    (recur (+ sz cur-size) (inc bat-n) part-n (conj acc (format "[%s,%s" sr ser)))
		    (async/>!! pipe [part-n (format "[%s,%s" sr ser)])))
	         ;; General case, accumulate serialized rows
	        (if (< cur-size byte-limit)
	          (recur cur-size (inc bat-n) part-n (conj acc ser))
                  (if (async/>!! pipe [part-n (prepare-part part-n (conj acc ser))])
		    (recur 0 (inc bat-n) (inc part-n) [])
		    (println "PIPE IS CLOSED: CAN'T WRITE")))))
            (do
	      (async/>!! pipe [part-n (prepare-part part-n acc "]")])
	      (async/close! pipe))))
	(catch Exception e
	  (println "Error reading records")
	  (println e)
	  (async/close! pipe))))))

(defn run [conf src-path dest-path-prefix col-headers col-handlers thread-count byte-limit bkt key]
  (let [pipe (async/chan buffer-size)]
    (try
      (start-worker pipe conf src-path col-headers col-handlers byte-limit)
      ;; Thread macro uses daemon threads so we must explicitly block
      ;; until all writer threads are complete to prevent premature
      ;; termination of main thread.
      (async/<!! (start-multipart-writer pipe thread-count (client (credentials)) dest-path-prefix bkt key))
      (catch Exception e
        (println e)))))


(defn orc->json [src-path dest-path-prefix col-headers col-handlers wrt-thread-count byte-limit bkt key]
  (run (orc-config) src-path dest-path-prefix col-headers col-handlers wrt-thread-count byte-limit bkt key))
