(ns org.rathore.amit.humpty-dumpty.persistence
  (:require redis)
  (:use org.rathore.amit.humpty-dumpty.utils)
  (:use clojure.contrib.str-utils)
  (:require (org.danlarkin [json :as json])))

(def LAST-ACCESSED-TIMES "last_accessed_times")

;; serialization
(defmulti serialize (fn [format key-type value]
		      [format key-type]))

(defmethod serialize [:json :string-type] [format key-type value]
  (json/encode-to-str value))

(defmethod serialize [:json :list-type] [format key-type value]
  (map json/encode-to-str value))

(defmethod serialize [:json :map-type] [format key-type value]
  (map json/encode-to-str value))

(defmethod serialize [:clj-str :string-type] [format key-type value]
  (pr-str value))

(defmethod serialize [:clj-str :list-type] [format key-type value]
  (map pr-str value))

(defmethod serialize [:clj-str :map-type] [format key-type value]
  (map pr-str value))


;; deserialization
(defmulti deserialize (fn [format key-type serialized]
			[format key-type]))

(defmethod deserialize [:json :string-type] [format key-type serialized]
  (json/decode-from-str serialized))

(defmethod deserialize [:json :list-type] [format key-type serialized]
  (map json/decode-from-str serialized))

(defmethod deserialize [:json :map-type] [format key-type serialized]
  (map json/decode-from-str serialized))

(defmethod deserialize [:clj-str :string-type] [format key-type serialized]
  (read-string serialized))

(defmethod deserialize [:clj-str :list-type] [format key-type serialized]
  (map read-string serialized))

(defmethod deserialize [:clj-str :map-type] [format key-type serialized]
  (map read-string serialized))

(def inserters {
  :string-type redis/set
  :list-type redis/rpush
  :map-type redis/rpush
})

(def fetchers {
  :string-type (fn [key] 
		 {key {:value (redis/get key) :key-type :string-type}})
  :list-type (fn [key]
	       {key {:value (redis/lrange key 0 (redis/llen key)) :key-type :list-type}})
  :map-type (fn [key]
	       {key {:value (redis/lrange key 0 (redis/llen key)) :key-type :map-type}})})

(defn insert-into-redis [persistable]
  (let [inserter (fn [[k v]]
		   (cond
		     (= (v :key-type) :string-type) ((inserters :string-type) k (v :value))
		     (= (v :key-type) :list-type) (doall (map #((inserters :list-type) k %) (v :value)))
                     (= (v :key-type) :list-type) (doall (map #((inserters :map-type) k %) (v :value)))))]
    (doall (map inserter persistable))))

(defn persistable-for [humpty key-vals]
  (let [dumpty (humpty :type)
        separator (dumpty :key-separator)
        format (dumpty :format)
        pk-value (humpty :primary-key-value)
        kv-persister (fn [[k v]]
                       (let [key-type (dumpty :key-type k)]
                         {(str pk-value separator k) 
                          {:value (serialize format key-type v)
                           :key-type key-type}}))]
    (apply merge (map kv-persister key-vals))))

(defn validate-for-persistence [humpty]
  (let [pk-values (humpty :primary-key-values)]
    (if (every? empty? pk-values)
      (throw (RuntimeException. (str (humpty :type-name) " must have a primary key value for persisting!"))))))

(declare stamp-update-time)

(defn persist
  ([humpty key-vals]
     (validate-for-persistence humpty)
     (let [ready-to-persist (persistable-for humpty key-vals)]
       (insert-into-redis ready-to-persist))
     (stamp-update-time humpty))
  ([humpty]
     (persist humpty (humpty :get-state))))

(defn deserialize-state [serialized dumpty]
  (let [format (dumpty :format)
	separator (dumpty :key-separator)
	key-from (fn [k] (read-string (last (.split k separator))))
	deserializer (fn [[k {:keys [key-type value]}]]
		       (if-not value
			 {}
			 {(key-from k) (deserialize format key-type value)}))]
    (apply merge (map deserializer serialized))))

(defn find-by-primary-key [dumpty pk-values]
  (let [string-keys (dumpty :string-keys pk-values)
	list-keys (dumpty :list-keys pk-values)
	string-maps (apply merge (map #((fetchers :string-type) %) string-keys))
	list-maps (apply merge (map #((fetchers :list-type) %) list-keys))
	serialized (merge string-maps list-maps)
	deserialized (deserialize-state serialized dumpty)]
    (if (every? empty? (vals deserialized))
      nil
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