(ns org.rathore.amit.humpty-dumpty.persistence
  (:require redis)
  (:use org.rathore.amit.humpty-dumpty.utils)
  (:use clojure.contrib.str-utils)
  (:require (org.danlarkin [json :as json])))

(def *EXPIRE-AFTER-MILLIS* (* 1000 60 60))

(defn validate-for-persistence [humpty]
  (let [pk-values (humpty :primary-key-values)]
    (if (every? empty? pk-values)
      (throw (RuntimeException. (str (humpty :type-name) " must have a primary key value for persisting!"))))))

(defn serialize [format key-vals]
  (condp = format
    :json (json/encode-to-str key-vals)
    :clj-str (pr-str key-vals)))

(defn deserialize [format serialized]
  (if-not serialized
    {}
    (condp = format
      :json (json/decode-from-str serialized)
      :clj-str (read-string serialized))))

(defn insert-into-redis [pk-value format key-vals]
  (let [seriazlized (serialize format key-vals)]
    (redis/set pk-value seriazlized)
    (redis/expire pk-value *EXPIRE-AFTER-MILLIS*)))

(defn persist
  ([humpty key-vals]
     (validate-for-persistence humpty)
     (let [d (humpty :type)]
       (insert-into-redis (humpty :primary-key-value) (d :format) key-vals)))
  ([humpty]
     (persist humpty (humpty :get-state))))

(defn find-by-primary-key [dumpty pk-values]
  (let [pk (str-join (dumpty :key-separator) pk-values)
        serialized (redis/get pk)
        deserialized (deserialize (dumpty :format) serialized)]
    (if-not (empty? deserialized)
      (dumpty :new-with-state deserialized))))

(defn destroy-by-primary-key [dumpty pk-values]
  (let [redis-keys-prefix (str-join (dumpty :key-separator) pk-values)
        keys-for-humpty (redis/keys (str redis-keys-prefix "*"))]
    (doseq [redis-key keys-for-humpty]
      (redis/del redis-key))))
