(ns org.rathore.amit.humpty-dumpty.persistence
  (:require redis)
  (:use org.rathore.amit.humpty-dumpty.utils)
  (:use clojure.contrib.str-utils)
  (:require (org.danlarkin [json :as json])))

(def LAST-ACCESSED-TIMES "last_accessed_times")

(defn validate-for-persistence [humpty]
  (let [pk-values (humpty :primary-key-values)]
    (if (every? empty? pk-values)
      (throw (RuntimeException. (str (humpty :type-name) " must have a primary key value for persisting!"))))))

(declare stamp-update-time)

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
    (redis/set pk-value seriazlized)))

(defn persist
  ([humpty key-vals]
     (validate-for-persistence humpty)
     (let [d (humpty :type)]
       (insert-into-redis (humpty :primary-key-value) (d :format) key-vals))
     (stamp-update-time humpty))
  ([humpty]
     (persist humpty (humpty :get-state))))

(defn find-by-primary-key [dumpty pk-values]
  (let [pk (str-join (dumpty :key-separator) pk-values)
        serialized (redis/get pk)
        deserialized (deserialize (dumpty :format) serialized)]
    (if-not (empty? deserialized)
      (dumpty :new-with-state deserialized))))

(defn stamp-update-time [humpty]
  (let [pk-value (humpty :primary-key-value)]
    (redis/zadd LAST-ACCESSED-TIMES (now-score) pk-value)))

(defn- last-accessed-time-as-date [pk-value]
  (if-let [now (redis/zscore LAST-ACCESSED-TIMES pk-value)]
    (score-date (str (.longValue now)))))

(defn get-last-updated [humpty]
  (let [pk-value (humpty :primary-key-value)]
    (last-accessed-time-as-date pk-value)))

(defn destroy-by-primary-key [dumpty pk-values]
  (let [redis-keys-prefix (str-join (dumpty :key-separator) pk-values)
        keys-for-humpty (redis/keys (str redis-keys-prefix "*"))]
    (doseq [redis-key keys-for-humpty]
      (redis/del redis-key))
    (redis/zrem LAST-ACCESSED-TIMES redis-keys-prefix)))
(defn- check-expiry [last-accessed ttl]
  (let [expires-at (add-seconds last-accessed ttl)
        now (score-date (now-score))]
    (.before expires-at now)))

(defn humpty-expired? [humpty]
  (let [last-accessed (humpty :last-updated)
        ttl ((humpty :type) :ttl)]
    (check-expiry last-accessed ttl)))

(defn dumpty-expired? [dumpty pk-values]
  (let [pk-value (str-join (dumpty :key-separator) pk-values)]
    (if-let [last-accessed (last-accessed-time-as-date pk-value)]
      (check-expiry last-accessed (dumpty :ttl)))))