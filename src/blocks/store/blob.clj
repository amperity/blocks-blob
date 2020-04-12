(ns blocks.store.blob
  "Block storage backed by Azure blob storage."
  (:require
    [blocks.data :as data]
    [blocks.store :as store]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [multihash.core :as multihash])
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
      SharedAccessBlobHeaders
      SharedAccessBlobPolicy)
    (java.io
      InputStream
      OutputStream)
    (java.net
      URI)))


(defn- id->path
  "Converts a multihash identifier to an Blob Storage object name, potentially applying a
  common root. Multihashes are rendered as hex strings."
  ^String
  [root id]
  (str root (multihash/hex id)))


(defn blob->stats
  [^CloudBlob blob]
  (let [^BlobProperties properties (.getProperties blob)
        name (last (str/split (.getName blob) #"/"))]
    {:id (multihash/decode name)
     :size (.getLength properties)
     :source (.getPrimaryUri (.getSnapshotQualifiedStorageUri blob))
     :stored-at (.getLastModified properties)}))


(defn file->block
  [^CloudBlobContainer container root stats]
  (let [id (:id stats)
        path (id->path root id)]
    (block/with-stats
      (data/lazy-block
        id (:size stats)
        (fn file-reader
          []
          (-> (.getBlockBlobReference container path)
              (.openInputStream))))
      (dissoc stats :id :size))))

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
      (assoc this :container (CloudBlobContainer. container-uri credentials))
      this))


  (stop
    [this]
    (if container
      (assoc this :container nil)
      this))


  store/BlockStore

  (-stat
    [this id]
    (let [path (id->path root id)
          ^CloudBlob blob (.getBlockBlobReference container path)]
      (when (and blob (.exists blob))
        (blob->stats blob))))


  (-list
    [this opts]
    (->> (.listBlobs container root true) ; lazy, flat list of blobs
         (map blob->stats)
         (store/select-stats opts)))


  (-get
    [this id]
    (when-let [stats (.-stat this id)]
      (file->block container root stats)))


  (-put!
    [this block]
    (let [path (id->path root (:id block))
          ^CloudBlob blob (.getBlockBlobReference container path)]
      (when-not (.exists blob)
        (with-open [output (.openOutputStream blob)
                    content (block/open block)]
          (io/copy content output)))
      (.-get this (:id block))))


  (-delete!
    [this id]
    (try
      (let [path (id->path root id)
            ^CloudBlob blob (.getBlockBlobReference container path)]
        (log/debugf "Deleting file %s" (.getPrimaryUri (.getSnapshotQualifiedStorageUri blob)))
        (.delete blob)
        true)
      (catch StorageException se
        (when (not= 404 (.getHttpStatusCode se))
          (throw se))
        false))))

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
  [container-uri ^StorageCredentials credentials & {:as opts}]
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
