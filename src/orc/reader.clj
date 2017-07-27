(ns orc.reader
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
	    [clojure.core.async :as async]
	    [orc.core :as core])
  (import [com.amazonaws AmazonClientException AmazonServiceException]
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


(def buffer-size 100)

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
	    ;; TODO: only abort if still processing
            (.abortMultipartUpload clnt (abort-multipart-upload-request bkt key uid))))))
    out-ch))

(defn run [conf src-path dest-path-prefix col-headers col-handlers thread-count byte-limit bkt key]
  (let [pipe (async/chan buffer-size)]
    (try
      (core/start-worker pipe conf src-path col-headers col-handlers byte-limit)
      ;; Thread macro uses daemon threads so we must explicitly block
      ;; until all writer threads are complete to prevent premature
      ;; termination of main thread.
      (async/<!! (start-multipart-writer pipe thread-count (client (credentials)) dest-path-prefix bkt key))
      (catch Exception e
        (println e)))))


(defn orc->json [src-path dest-path-prefix col-headers col-handlers wrt-thread-count byte-limit bkt key]
  (run (core/orc-config) src-path dest-path-prefix col-headers col-handlers wrt-thread-count byte-limit bkt key))
