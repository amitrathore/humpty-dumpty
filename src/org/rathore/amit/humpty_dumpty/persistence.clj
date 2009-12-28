(ns org.rathore.amit.humpty-dumpty.persistence
  (require (org.danlarkin [json :as json])))

(defn serialize [format value]
  (cond 
    (= format :json) (json/encode-to-str value)
    (= format :clj-str) (pr-str value)))

(defn deserialize [format serialized]
  (cond 
    (= format :json) (json/decode-from-str serialized)
    (= format :clj-str) (read-string serialized)))

(defn insert-into-redis [state]
)

(defn persistable-for [humpty]
  (let [dumpty (humpty :type)
	separator (dumpty :key-separator)
	format (dumpty :format)
	pk-value (humpty :primary-key-value)
	kv-persister (fn [[k v]]
		       {(str pk-value separator k) (serialize format v)})]
    (apply merge (map kv-persister (humpty :get-state)))))

(defn persist [humpty]
  (let [ready-to-persist (persistable-for humpty)]
    (insert-into-redis ready-to-persist)))
