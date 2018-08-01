(ns blocks.store.blob-test
  (:require
    [blocks.core :as block]
    [blocks.store.blob :as blob :refer [blob-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]
    [multihash.core :as multihash])
  (:import
    (com.microsoft.azure.storage
      StorageCredentialsSharedAccessSignature)
    (java.net
      URI)))


;; ## Integration Tests

(def blob-uri (System/getenv "BLOCKS_BLOB_TEST_URI"))
(def blob-sas-token (System/getenv "BLOCKS_BLOB_TEST_SAS_TOKEN"))

(deftest ^:integration check-blob-store
  (if-let [test-store-uri blob-uri]
    (let [credentials (StorageCredentialsSharedAccessSignature. blob-sas-token)
          root-path (str "testing/blocks/test-" (rand-int 1000))
          counter (atom 0)]
      ; NOTE: can run concurrent tests by making this `check-store*` instead.
      (tests/check-store
        (fn [& _]
          (let [path (format "%s/%08d" root-path (swap! counter inc))
                store (blob-block-store (URI. test-store-uri)
                                        credentials
                                        :root path)]
            store))))
    (println "No BLOCKS_BLOB_TEST_URI in environment, skipping integration test!")))
