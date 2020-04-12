(ns blocks.store.blob-test
  (:require
    [blocks.core :as block]
    [blocks.store.blob :as blob :refer [blob-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer [deftest]]
    [com.stuartsierra.component :as component]
    [multiformats.hash :as multihash])
  (:import
    (com.microsoft.azure.storage
      StorageCredentialsSharedAccessSignature)
    java.net.URI))


;; ## Integration Tests

(def blob-uri (System/getenv "BLOCKS_BLOB_TEST_URI"))


(deftest ^:integration check-blob-store
  (if-let [uri (some-> blob-uri URI.)]
    (let [storage-uri (URI. "https" (.getHost uri) (.getPath uri) nil)
          credentials (StorageCredentialsSharedAccessSignature. (.getRawQuery uri))
          base-path (str "greg/blocks/test-" (rand-int 1000))
          counter (atom 0)]
      ;; NOTE: can run concurrent tests by making this `check-store*` instead.
      (tests/check-store
        (fn [& _]
          (let [path (format "%s/%08d/" base-path (swap! counter inc))]
            (component/start
              (blob-block-store
                storage-uri credentials
                :root path))))))
    (println "No BLOCKS_BLOB_TEST_URI in environment, skipping integration test!")))
