(ns user
  (:require
    [blocks.core :as block]
    [blocks.store :as store]
    [blocks.store.blob :refer [blob-block-store]]
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.namespace.repl :refer [refresh]]
    [com.stuartsierra.component :as component]
    [multiformats.hash :as multihash])
  (:import
    (com.microsoft.azure.storage
      StorageCredentialsSharedAccessSignature)
    java.net.URI))


(defn new-store
  [path]
  (some->
    (System/getenv "BLOCKS_BLOB_TEST_URI")
    (block/->store)
    (assoc :root (if (str/ends-with? path "/")
                   path
                   (str path "/")))
    (component/start)))
