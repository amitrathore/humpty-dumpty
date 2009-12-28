;; a humpty is the object that will be persisted into redis
;; a dumpty is the blue-print for a humpty

(ns org.rathore.amit.humpty-dumpty.core
  (:require redis)
  (use clojure.contrib.str-utils))

(def *redis-server-spec* {})

(defn persist [state separator fornat]
  )

(defn primary-key-value [humpty-obj]
  (let [pk-keys ((humpty-obj :type) :primary-key)
	separator ((humpty-obj :type) :key-separator)
	values (map #(humpty-obj :get %) pk-keys)]
    (str-join separator values)))

(defn new-humpty [dumpty]
  (let [state (ref {})]
    (fn thiz [accessor & args]
      (redis/with-server *redis-server-spec*
	(cond
	  (= :type accessor) dumpty
	  (= :set! accessor) (let [[k v] args] 
			       (dosync
				(alter state assoc k v))
			       v)
	  (= :add! accessor) (let [[k v] args
				   add-to-inner-list (fn [current-state ke valu] 
						       (update-in current-state [ke] conj valu))]
			       (dosync
				(alter state add-to-inner-list k v)))
	  (= :get accessor) (let [[k] args]
			      (state k))
	  (= :primary-key-value accessor) (primary-key-value thiz)
	  (= :save! accessor) (persist @state (dumpty :key-separator) (dumpty :format))
	  :else (throw (RuntimeException. (str "Unknown message " accessor " sent to humpty-dumpty object of type " (dumpty :name)))))))))

(defn new-dumpty [name separator format primary-keys string-attribs set-attribs]
  (fn dumpty [accessor & args]
    (redis/with-server *redis-server-spec*
      (cond
	(= :name accessor) name
	(= :format accessor) format
	(= :key-separator accessor) separator
	(= :primary-key accessor) primary-keys
	(= :new accessor) (new-humpty dumpty)
	:else (throw (RuntimeException. (str "Unknown commmand " accessor " sent to " name)))))))

(defn specs-for [redis-datatype specs]
  (let [type-spec? #(= redis-datatype (first %))
	extractor (comp next first)]
    (extractor (filter type-spec? specs))))

(defmacro defdumpty [name & specs]
  (let [string-types (specs-for 'string-type specs)
	list-types (specs-for 'list-type specs)
	pk-keys (specs-for 'primary-key specs)
	format (or (first (specs-for 'format-type specs)) :clj-str)
	separator (or (first (specs-for 'key-separator specs)) "___")]
    `(def ~name 
	  (new-dumpty '~name ~separator ~format '~pk-keys '~string-types '~list-types))))
