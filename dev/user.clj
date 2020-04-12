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
    [multihash.core :as multihash]
    [multihash.digest :as digest])
  (:import
    (com.microsoft.azure.storage
      StorageCredentialsSharedAccessSignature)
    (java.net
      URI)))


(defn store
  [path]
  (let [container-uri (URI. (System/getenv "BLOCKS_BLOB_TEST_URI"))
        shared-access-key (StorageCredentialsSharedAccessSignature. (System/getenv "BLOCKS_BLOB_TEST_SAS_TOKEN"))]
    (->
      (blob-block-store container-uri shared-access-key
                        :root (or path "user"))
      (component/start))))
