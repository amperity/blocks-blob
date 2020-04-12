(ns blocks.store.blob
  "Block storage backed by Azure blob storage."
  (:require
    [blocks.data :as data]
    [blocks.store :as store]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [manifold.stream :as s]
    [multiformats.hash :as multihash])
  (:import
    (com.microsoft.azure.storage
      StorageCredentials
      StorageCredentialsSharedAccessSignature
      StorageException)
    (com.microsoft.azure.storage.blob
      BlobProperties
      CloudBlob
      CloudBlobClient
      CloudBlobContainer
      CloudBlockBlob
      ListBlobItem
      SharedAccessBlobHeaders
      SharedAccessBlobPolicy)
    (java.io
      InputStream
      OutputStream)
    java.net.URI
    java.time.Instant
    java.util.Date
    (org.apache.commons.io.input
      BoundedInputStream)))


;; ## Blob Functions

(defn- hex?
  "True if the value is a valid hexadecimal string."
  [x]
  (and (string? x) (re-matches #"[0-9a-fA-F]+" x)))


(defn- id->path
  "Converts a multihash identifier to an Blob Storage object name, potentially applying a
  common root. Multihashes are rendered as hex strings."
  ^String
  [root id]
  (str root (multihash/hex id)))


(defn- blob->stats
  "Return the stats map (with metadata) for a blob."
  [^CloudBlob blob]
  (let [properties (.getProperties blob)
        blob-name (last (str/split (.getName blob) #"/"))]
    (when (hex? blob-name)
      (with-meta
        {:id (multihash/parse blob-name)
         :size (.getLength properties)
         :stored-at (if-let [date (or (.getLastModified properties)
                                      (.getCreatedTime properties))]
                      (.toInstant ^Date date)
                      (Instant/now))}
        {::source (.getPrimaryUri (.getSnapshotQualifiedStorageUri blob))
         ::name (.getName blob)}))))



;; ## Blob Content

(deftype BlobReader
  [^CloudBlockBlob blob]

  data/ContentReader

  (read-all
    [this]
    (log/trace "Opening Azure blob" (.getName blob))
    #_(.getBlockBlobReference container blob-name)
    (.openInputStream blob))


  (read-range
    [this start end]
    (log/tracef "Opening Azure blob %s byte range %s - %s"
                (.getName blob)
                (or start "start")
                (or end "end"))
    (.openInputStream blob
                      (long (or start 0))
                      (and end (- end (or start 0)))
                      nil nil nil)))


(alter-meta! #'->BlobReader assoc :private true)


(defn- blob->block
  "Construct a new block from the given block blob reference."
  [blob stats]
  (let [stat-meta (meta stats)]
    (with-meta
      (data/create-block
        (:id stats)
        (:size stats)
        (:stored-at stats)
        (->BlobReader blob))
      stat-meta)))



;; ## Blob Iteration

(defn- run-blobs!
  "Run the provided function over all blobs in the container matching the given
  query. The loop will terminate if the function returns false or nil."
  [^CloudBlobContainer container path query f]
  (let [{:keys [after before]} query]
    ;; OPTIMIZE: if after/before share a common prefix we can narrow the range here.
    (loop [blobs (.iterator (.listBlobs container path true))
           limit (:limit query)]
      (when-let [blob (and (or (nil? limit) (pos? limit))
                           (.hasNext blobs)
                           (.next blobs))]
        (if-let [stats (blob->stats blob)]
          (cond
            ;; Skip block which occurs before :after marker.
            (and after (not (pos? (compare (multihash/hex (:id stats)) after))))
            (recur blobs limit)

            ;; Terminate when encountering blocks after :before marker.
            (and before (not (neg? (compare (multihash/hex (:id stats)) before))))
            nil

            ;; Otherwise, call function.
            (f blob)
            (recur blobs (when limit (dec limit)))

            ;; Function returned false, halt iteration.
            :else nil)
          ;; Ignore non-block blob.
          (recur blobs limit))))))



;; ## Block Store

(defrecord BlobBlockStore
  [^URI container-uri
   ^StorageCredentials credentials
   ^CloudBlobContainer container
   ^String root]

  component/Lifecycle

  (start
    [this]
    (if-not container
      (do
        (log/info "Connecting to Azure blob container" container-uri)
        (assoc this :container (CloudBlobContainer. container-uri credentials)))
      this))


  (stop
    [this]
    (if container
      (assoc this :container nil)
      this))


  store/BlockStore

  (-list
    [this opts]
    (let [out (s/stream 1000)]
      (store/future'
        (try
          (run-blobs!
            container root opts
            (fn stream-block
              [blob]
              (if-let [stats (and (instance? CloudBlockBlob blob)
                                  (blob->stats blob))]
                ;; Publish block to stream.
                @(s/put! out (blob->block blob stats))
                ;; Doesn't look like a block - ignore and continue.
                true)))
          (catch Exception ex
            (log/error ex "Failure listing blob container")
            (s/put! out ex))
          (finally
            (s/close! out))))
      (s/source-only out)))


  (-stat
    [this id]
    (store/future'
      (let [path (id->path root id)
            blob (.getBlockBlobReference container path)]
        (when (and blob (.exists blob))
          (blob->stats blob)))))


  (-get
    [this id]
    (store/future'
      (let [path (id->path root id)
            blob (.getBlockBlobReference container path)]
        (when-let [stats (and blob (.exists blob) (blob->stats blob))]
          (blob->block blob stats)))))


  (-put!
    [this block]
    (store/future'
      (let [path (id->path root (:id block))
            blob (.getBlockBlobReference container path)]
        (if (.exists blob)
          ;; Read existing blob.
          (let [stats (blob->stats blob)]
            (blob->block blob stats))
          ;; Write blob.
          (do
            (with-open [output (.openOutputStream blob)
                        content (data/content-stream block nil nil)]
              (io/copy content output))
            (blob->block
              blob
              (with-meta
                {:id (:id block)
                 :size (:size block)
                 :stored-at (Instant/now)}
                {::source (.getPrimaryUri (.getSnapshotQualifiedStorageUri blob))
                 ::name path})))))))


  (-delete!
    [this id]
    (store/future'
      (try
        (let [path (id->path root id)
              blob (.getBlockBlobReference container path)]
          (.delete blob)
          true)
        (catch StorageException se
          (when (not= 404 (.getHttpStatusCode se))
            (throw se))
          false)))))



;; ## Store Construction

(store/privatize-constructors! BlobBlockStore)


(defn- canonical-root
  "Ensures that a root path doesn't begin with a slash but does end with one."
  [root]
  (if (= "/"  root)
    ""
    (as-> root path
          (if (str/starts-with? path "/")
            (subs path 1 (count path))
            path)
          (if (str/ends-with? path "/")
            path
            (str path "/")))))


(defn blob-block-store
  [container-uri credentials & {:as opts}]
  (map->BlobBlockStore
    (assoc opts
           :credentials credentials
           :container-uri container-uri
           :root (canonical-root (:root opts "/")))))


(defmethod store/initialize "blob"
  [location]
  (let [uri (URI. location)]
    (blob-block-store
      (URI. "https" (.getHost uri) (.getPath uri) nil)
      (StorageCredentialsSharedAccessSignature. (.getRawQuery uri)))))
