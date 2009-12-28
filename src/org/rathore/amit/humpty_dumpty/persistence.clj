(ns org.rathore.amit.humpty-dumpty.persistence
  (:require redis)
  (:require (org.danlarkin [json :as json])))

;; serialization
(defmulti serialize (fn [format key-type value]
		      [format key-type]))

(defmethod serialize [:json :string-type] [format key-type value]
  (json/encode-to-str value))

(defmethod serialize [:json :list-type] [format key-type value]
  (map json/encode-to-str value))

(defmethod serialize [:clj-str :string-type] [format key-type value]
  (pr-str value))

(defmethod serialize [:clj-str :list-type] [format key-type value]
  (map pr-str value))


;; deserialization
(defmulti deserialize (fn [format key-type serialized]
			[format key-type]))

(defmethod deserialize [:json :string-type] [format key-type serialized]
  (json/decode-from-str serialized))

(defmethod deserialize [:json :list-type] [format key-type serialized]
  (map json/decode-from-str serialized))

(defmethod deserialize [:clj-str :string-type] [format key-type serialized]
  (read-string serialized))

(defmethod deserialize [:clj-str :list-type] [format key-type serialized]
  (map read-string serialized))

(def inserters {
  :string-type redis/set
  :list-type redis/rpush
})

(def fetchers {
  :string-type (fn [key] 
		 {key {:value (redis/get key) :key-type :string-type}})
  :list-type (fn [key]
	       {key {:value (redis/lrange key 0 (redis/llen key)) :key-type :list-type}})})

(defn insert-into-redis [persistable]
  (let [inserter (fn [[k v]]
		   (cond
		     (= (v :key-type) :string-type) ((inserters :string-type) k (v :value))
		     (= (v :key-type) :list-type) (doall (map #((inserters :list-type) k %) (v :value)))))]
    (doall (map inserter persistable))))

(defn persistable-for [humpty]
  (let [dumpty (humpty :type)
	separator (dumpty :key-separator)
	format (dumpty :format)
	pk-value (humpty :primary-key-value)
	kv-persister (fn [[k v]]
		       (let [key-type (dumpty :key-type k)]
			 {(str pk-value separator k) 
			  {:value (serialize format key-type v)
			   :key-type key-type}}))]
    (apply merge (map kv-persister (humpty :get-state)))))

(defn persist [humpty]
  (let [ready-to-persist (persistable-for humpty)]
    (insert-into-redis ready-to-persist)))

(defn find-by-primary-key [dumpty pk-values]
  (let [string-keys (dumpty :string-keys pk-values)
	list-keys (dumpty :list-keys pk-values)
	string-maps (apply merge (map #((fetchers :string-type) %) string-keys))
	list-maps (apply merge (map #((fetchers :list-type) %) list-keys))
	serialized (merge string-maps list-maps)]
    (println serialized)))